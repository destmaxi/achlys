%%%-------------------------------------------------------------------
%%% @author Maxime De Streel, Benjamin Simon
%%% @doc The Pmod_temp worker server.
%%% The general purpose of this worker is to gather
%%% and process sensor data from the temperature sensor.
%%%
%%%   Data can be retrieved as follows :
%%%
%%%   [Temperature] = pmod_nav:read(acc, [out_temp]).
%%%
%%%   Where <em>acc</em> is the component providing the
%%%   data and <em>[out_temp]</em> is the list of registers
%%%   that is read.
%%%   @see pmod_nav. <b>Pmod_NAV</b>
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(achlys_pmod_temp_worker).
-author("Maxime De Streel, Benjamin Simon").

-behaviour(gen_server).

-include("achlys.hrl").

%%====================================================================
%% API
%%====================================================================

-export([start_link/0]).
-export([run/0]).

%%====================================================================
%% Gen Server Callbacks
%%====================================================================

-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  handle_continue/2,
  terminate/2,
  code_change/3]).

%%====================================================================
%% Macros
%%====================================================================

-define(SERVER, ?MODULE).
-define(PMOD_NAV_SLOT, spi1).
-define(TIME, erlang:monotonic_time()).

%%====================================================================
%% Records
%%====================================================================

-record(state, {
  measures :: map(),
  round :: pos_integer(),
  cardinality :: pos_integer()
}).

-type state() :: #state{}.

-type pmod_nav_status() :: {ok, pmod_nav}
| {error, no_device | no_pmod_nav | unknown}.


%%====================================================================
%% API
%%====================================================================

% @doc starts the pmod_temp process using the configuration
% given in the sys.config file.
-spec start_link() ->
  {ok, pid()} | ignore | {error, {already_started, pid()} | term()}.
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc declares a Lasp variable for the global round
%% and sets triggers for handlers after intervals have expired.
-spec run() -> ok.
run() ->
  gen_server:cast(?SERVER, run).


%%====================================================================
%% Gen Server Callbacks
%%====================================================================

% @private
-spec init([]) -> {ok, state()}.
init([]) ->
  _ = rand:seed(exrop),
  MeasuresParameter = achlys_config:get(temperature_worker, #{}),
  % erlang:send_after(?TEN, ?SERVER, run),
  {ok, #state{
    measures = MeasuresParameter,
    round = 0,
    cardinality = 2}}.


%%--------------------------------------------------------------------

% @private
handle_call(_Request, _From, State) ->
  {reply, ignored, State}.

%%--------------------------------------------------------------------

% @private
handle_cast(run, State) ->
  logger:log(notice, "Declared CRDTs for global round ~n"),

  {ok, {Id, _, _, _}} = lasp:declare({erlang:atom_to_binary(temperature_worker_counter, utf8)
    , state_max_int}, state_max_int),

  IntermediateState = mapz:deep_put([global_round], Id, State#state.measures),

  T = achlys_util:create_table(temperature),
  NewState = mapz:deep_put([table], T, IntermediateState),

  #{poll_interval := P
    , aggregation_trigger := A} = NewState,
  erlang:send_after(((P * A) + ?THREE), ?SERVER, aggregate),
  erlang:send_after(?ONE, ?SERVER, poll),
  {noreply, State#state{measures = NewState}};


%%--------------------------------------------------------------------

% @private
handle_cast(_Msg, State) ->
  {noreply, ignored, State}.

%%--------------------------------------------------------------------

%% @doc fetches the values from the {@link pmod_nav} temperature sensor
%% and stores them in the corresponding ETS table. It is paired with
%% the {@link erlang:monotonic_time/0} to guarantee unique keys.
%% For large amounts of sensor data
%% e.g. accumulated for a long time and being larger than
%% the maximum available memory, an alternative would be to use the
%% {@link dets} storage module. They can also be combined as described
%% below.
%%
%% From OTP documentation :
%%
%% Dets tables provide efficient file-based Erlang term storage.
%% They are used together with ETS tables when fast access
%% needs to be complemented with persistency.

handle_info(poll, State) ->
  Res = maybe_get_temp(),
  case Res of
    {ok, [Temp]} when is_number(Temp) ->
      true = ets:insert_new(temperature, {?TIME, erlang:round(Temp)});
    _ ->
      logger:log(notice, "Could not fetch temperature : ~p ~n", [Res])
  end,
  erlang:send_after(mapz:deep_get([poll_interval], State#state.measures)
    , ?SERVER
    , poll),
  {noreply, State, hibernate};


%%--------------------------------------------------------------------
handle_info(aggregate, State) ->
  io:fwrite("Start ~p ~n", [1]),
  #{global_round := GR
    , table := T
    , poll_interval := P
    , aggregation_trigger := A} = State#state.measures,
  NewRound = update_counter(GR, State),
  io:fwrite("Round ~p ~n", [NewRound]),
  Len = ets:info(temperature, size),
  _ = case Len >= A of
        true ->
          {_Sample, Mean} = get_mean(temperature),
          SetId = get_variable_identifier(NewRound),
          {ok, {Id, _, _, _}} = lasp:declare({SetId
            , state_awset}, state_awset),
          {ok, {C2, _, _, _}} = lasp:update(
            Id,
            {add, {node(), erlang:round(Mean)}},
            self()),
          wait_for_data(State#state.cardinality, C2, NewRound);
        _ ->
          logger:log(notice, "Could not compute aggregate with ~p values ~n", [Len])
      end,
  ok = achlys_cleaner:flush_table(T),

  erlang:send_after((P * A), ?SERVER, aggregate),

  io:fwrite("End ~p ~n", [1]),

  {noreply, State#state{round = NewRound}};

handle_info({analyse, Round, Id}, State) ->
  io:fwrite("Statrt ~p ~n", [2]),
  {ok, S} = lasp:query(Id),
  Fetched = sets:to_list(S),
  io:fwrite("Cardinality: ~p ~n", [{Round, State#state.cardinality}]),
  io:fwrite("Data: ~p ~n", [Fetched]),
  io:fwrite("End ~p ~n", [2]),
  {noreply, State};



handle_info({increment_cardinality, NewCardinality}, State) when NewCardinality > State#state.cardinality ->
  {noreply, State#state{cardinality = NewCardinality}};

handle_info({increment_cardinality, _}, State) ->
  {noreply, State};

handle_info({decrement_cardinality, NewCardinality}, State) when NewCardinality < State#state.cardinality ->
  {noreply, State#state{cardinality = NewCardinality}};

handle_info({decrement_cardinality, _}, State) ->
  {noreply, State};


handle_info(Info, State) ->
  logger:log(notice, "Info ~p values ~n", [Info]),
  {noreply, State}.

%%--------------------------------------------------------------------

%% This function is called by a gen_server process
%% whenever a previous callback returns {continue, Continue}.
%% handle_continue/2 is invoked immediately after the previous callback,
%% which makes it useful for performing work after initialization
%% or for splitting the work in a callback in multiple steps,
%% updating the process state along the way.

%%--------------------------------------------------------------------

handle_continue(_Continue, State) ->
  % {noreply,NewState} | {noreply,NewState,Timeout}
  % | {noreply,NewState,hibernate}
  % | {noreply,NewState,{continue,Continue}}
  % | {stop,Reason,NewState}
  {noreply, State}.

%%--------------------------------------------------------------------

terminate(_Reason, _State) ->
  % ok = ets:tab2file(temperature, "temperature"),
  % ok = ets:tab2file(pressure, "pressure"),
  dets:sync(node()),
  ok.

%%--------------------------------------------------------------------

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


%%====================================================================
%% Internal functions
%%====================================================================

%% @doc Returns the temperature average
%% based on entries in the ETS table
-spec get_mean(atom() | ets:tid()) -> {number(), float()}.
get_mean(Tab) ->
  Sum = ets:foldl(fun
                    (Elem, AccIn) ->
                      {_, Temp} = Elem,
                      Temp + AccIn
                  end, 0, Tab),
  Len = ets:info(Tab, size),
  {Len, (Sum / Len)}.

%% @doc Returns the current temperature
%% if a Pmod_NAV module is active on slot SPI1
-spec maybe_get_temp() -> {ok, [float()]} | pmod_nav_status().
maybe_get_temp() ->
  {Code, Val} = is_pmod_nav_alive(),
  case {Code, Val} of
    {ok, pmod_nav} ->
      {ok, pmod_nav:read(acc, [out_temp])};
    _ ->
      {Code, Val}
  end.


%% @doc Checks the SPI1 slot of the GRiSP board
%% for presence of a Pmod_NAV module.
-spec is_pmod_nav_alive() -> pmod_nav_status().
is_pmod_nav_alive() ->
  try grisp_devices:slot(?PMOD_NAV_SLOT) of
    {device, ?PMOD_NAV_SLOT, Device, _Pid, _Ref} when Device =:= pmod_nav ->
      {ok, pmod_nav};
    {device, ?PMOD_NAV_SLOT, Device, _Pid, _Ref} when Device =/= pmod_nav ->
      {error, no_pmod_nav}
  catch
    error:{no_device_connected, ?PMOD_NAV_SLOT} ->
      {error, no_device};
    _:_ ->
      {error, unknown}
  end.


get_variable_identifier(Round) ->
  unicode:characters_to_binary([erlang:atom_to_binary(temperature_worker, utf8), "_",
    list_to_binary(integer_to_list(Round))], utf8).

update_counter(GR, State) ->
  {ok, GCounter} = lasp:query(GR),
  if GCounter == State#state.round ->
    {ok, _} = lasp:update(GR, increment, self()),
    State#state.round + 1;
    true -> GCounter
  end.


wait_for_data(Cardinality, Id, Round) ->
  spawn(fun() ->
    Self = self(),
    _Pid = spawn(fun() ->
      lasp:read(Id, {cardinality, Cardinality}),
      Self ! {self(), ok} end),
    receive
      {_Pid, ok} ->
        try_increase_cardinality(Cardinality, Id),
        erlang:send(?SERVER, {analyse, Round, Id}),
        ok
    after
      40000 ->
        erlang:send(?SERVER, {decrement_cardinality, Cardinality - 1}),
        erlang:send(?SERVER, {analyse, Round, Id}),
        ok
    end,
    exit(terminated)
        end),
  ok.

try_increase_cardinality(Cardinality, Id) ->
  spawn(fun() ->
    Self = self(),
    _Pid = spawn(fun() ->
      lasp:read(Id, {cardinality, Cardinality + 1}),
      Self ! {self(), ok} end),
    receive
      {_Pid, ok} ->
        erlang:send(?SERVER, {increment_cardinality, Cardinality + 1}),
        ok
    after
      40000 ->
        ok
    end,
    exit(terminated)
        end),
  ok.