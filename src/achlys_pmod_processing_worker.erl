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

-module(achlys_pmod_processing_worker).
-author("Maxime De Streel, Benjamin Simon").

-behaviour(gen_server).

-include("achlys.hrl").

%%====================================================================
%% API
%%====================================================================

-export([start_link/0]).
-export([run/0]).
-export([pull/0,
  pull_and_remove/0,
  pull_last/0,
  pull_last/1]).

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
-define(PMOD_ALS_SLOT, spi2).
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

-type pmod_als_status() :: {ok, pmod_als}
| {error, no_device | no_pmod_als | unknown}.


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

pull() ->
  SetId = {erlang:atom_to_binary(analysed_data, utf8), state_awset},
  {ok, S} = lasp:query(SetId),
  sets:to_list(S).

pull_and_remove() ->
  SetId = {erlang:atom_to_binary(analysed_data, utf8), state_awset},
  {ok, S} = lasp:query(SetId),
  Data = sets:to_list(S),
  {ok, {_, _, _, _}} = lasp:update(SetId, {rmv_all, Data}, self()),
  Data.

pull_last() ->
  last(pull()).

pull_last(Value) when is_atom(Value) ->
  pull_last([Value]);
pull_last(Value) when is_tuple(Value) ->
  pull_last(tuple_to_list(Value));
pull_last(Value) when is_list(Value) ->
  [filter(NodeData, Value) || NodeData <- pull_last()].

last(Data) ->
  {LastRound, _, _} = lists:max(Data),
  [{Node, Values} || {Round, Node, Values} <- Data, Round =:= LastRound].

filter({Node, Data}, Values) ->
  filter(Data, Values, {Node}).

filter(Data, [H | T], Acc) ->
  Element = [Value || {Type, Value} <- Data, Type =:= H],
  case Element of
    [H1 | _] -> filter(Data, T, erlang:append_element(Acc, H1));
    [] -> filter(Data, T, erlang:append_element(Acc, not_avalaible))
  end;

filter(_, [], Acc) ->
  Acc.

%%====================================================================
%% Gen Server Callbacks
%%====================================================================

% @private
-spec init([]) -> {ok, state()}.
init([]) ->
  _ = rand:seed(exrop),
  MeasuresParameter = achlys_config:get(processing_worker, #{}),
  % erlang:send_after(?TEN, ?SERVER, run),
  {ok, #state{
    measures = MeasuresParameter,
    round = 0,
    cardinality = mapz:deep_get([number_of_nodes], MeasuresParameter)}}.


%%--------------------------------------------------------------------

% @private
handle_call(_Request, _From, State) ->
  {reply, ignored, State}.

%%--------------------------------------------------------------------

% @private
handle_cast(run, State) ->
  logger:log(notice, "Declared CRDTs for global round ~n"),

  CounterID = {erlang:atom_to_binary(temperature_worker_counter, utf8), state_max_int},
  {ok, {Id, _, _, _}} = lasp:declare(CounterID, state_max_int),

  IntermediateState = mapz:deep_put([global_round], Id, State#state.measures),

  SetId = {erlang:atom_to_binary(analysed_data, utf8), state_awset},
  {ok, {Id2, _, _, _}} = lasp:declare(SetId, state_awset),

  IntermediateState2 = mapz:deep_put([crdt], Id2, IntermediateState),


  T = [achlys_util:create_table(X) || X <- mapz:deep_get([collect], State#state.measures), is_atom(X)] ++
    [achlys_util:create_table(X) || {X, _, _} <- mapz:deep_get([collect], State#state.measures), is_atom(X)],
  NewState = mapz:deep_put([table], T, IntermediateState2),

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
  [poll(X) || X <- mapz:deep_get([collect], State#state.measures)],
  erlang:send_after(mapz:deep_get([poll_interval], State#state.measures)
    , ?SERVER
    , poll),
  {noreply, State, hibernate};


%%--------------------------------------------------------------------
handle_info(aggregate, State) ->
  #{global_round := GR
    , table := T
    , poll_interval := P
    , aggregation_trigger := A
    , timeout := To} = State#state.measures,
  NewRound = update_counter(GR, State),
  io:fwrite("Round ~p ~n", [NewRound]),

  Values = aggregate(mapz:deep_get([collect], State#state.measures), A),

  SetId = {get_variable_identifier(processing_worker, NewRound), state_awset},
  {ok, {Id, _, _, _}} = lasp:declare(SetId, state_awset),
  C2 = lasp_update_temporary(Id, {node(), Values}, 2 * To),

  wait_for_data(State#state.cardinality, C2, NewRound, To),
  [ok = achlys_cleaner:flush_table(X) || X <- T],

  erlang:send_after((P * A), ?SERVER, aggregate),

  {noreply, State#state{round = NewRound}};

handle_info({analyse, Round, Id}, State) ->
  #{crdt := C,
    keep_in_memory := To} = State#state.measures,
  {ok, S} = lasp:query(Id),
  Fetched = sets:to_list(S),
  io:fwrite("Cardinality: ~p ~n", [{Round, State#state.cardinality}]),
  io:fwrite("Data: ~p ~n", [Fetched]),
  analyse(mapz:deep_get([compute], State#state.measures), Fetched, Round, C, To),
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
  % dets:sync(node()),
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
                      add(Temp, AccIn)
                  end, 0, Tab),
  Len = ets:info(Tab, size),
  case Sum of
    {X, Y, Z} -> {Len, {(X / Len), (Y / Len), (Z / Len)}};
    _ -> {Len, (Sum / Len)}
  end.

add({X1, Y1, Z1}, {X2, Y2, Z2}) ->
  {X1 + X2, Y1 + Y2, Z1 + Z2};

add({X1, Y1, Z1}, 0) ->
  {X1, Y1, Z1};

add(A, B) ->
  A + B.

poll(Value) when is_atom(Value)->
  Res = read_sensor(Value),
  case Res of
    {ok, [Measure]} ->
      true = ets:insert_new(Value, {?TIME, Measure});
    {ok, [X, Y, Z]} ->
      true = ets:insert_new(Value, {?TIME, {X, Y, Z}});
    _ ->
      logger:log(notice, "Could not fetch data : ~p ~n", [Res])
  end;

poll({Value, {XMin, YMin, ZMin}, {XMax, YMax, ZMax}}) when is_atom(Value)->
  Res = read_sensor(Value),
  case Res of
    {ok, [X, Y, Z]} when X > XMin, Y > YMin, Z > ZMin, X < XMax, Y< YMax, Z < ZMax->
      true = ets:insert_new(Value, {?TIME, {X, Y, Z}});
    _ ->
      logger:log(notice, "Could not fetch data : ~p ~n", [Res])
  end;

poll({Value, Min, Max}) when is_atom(Value)->
  Res = read_sensor(Value),
  case Res of
    {ok, [Measure]} when Measure > Min, Measure < Max ->
      true = ets:insert_new(Value, {?TIME, Measure});
    _ ->
      logger:log(notice, "Could not fetch data : ~p ~n", [Res])
  end.

read_sensor(Value) ->
  if Value =:= temperature -> maybe_get_temp();
    Value =:= pressure -> maybe_get_press();
    Value =:= accelerometry -> maybe_get_acc();
    Value =:= gyroscopy -> maybe_get_gyr();
    Value =:= magnetic_field -> maybe_get_mag();
    Value =:= light -> maybe_get_light();
    true -> not_implemented
  end.

aggregate(Values, A) ->
  aggregate(Values, A, #{}).

aggregate([Value | T], A, Acc) when is_atom(Value) ->
  Len = ets:info(Value, size),
  case Len >= A of
    true ->
      {_Sample, Mean} = get_mean(Value),
      aggregate(T, A, mapz:deep_put([Value], Mean, Acc));
    _ ->
      logger:log(notice, "Could not compute aggregate with ~p values ~n", [Len]),
      aggregate(T, A, mapz:deep_put([Value], not_available, Acc))
  end;

aggregate([{Value, _, _} | T], A, Acc) when is_atom(Value) ->
  Len = ets:info(Value, size),
  case Len >= A of
    true ->
      {_Sample, Mean} = get_mean(Value),
      aggregate(T, A, mapz:deep_put([Value], Mean, Acc));
    _ ->
      logger:log(notice, "Could not compute aggregate with ~p values ~n", [Len]),
      aggregate(T, A, mapz:deep_put([Value], not_available, Acc))
  end;

aggregate([], _, Acc) ->
  Acc.

analyse(Computations, Data, Round, Id, To) ->
  analyse(Computations, Data, Round, Id, To, []).

analyse([H | T], Data, Round, Id, To, Acc) ->
  case H of
    {Value, Computations} when is_atom(Value), is_list(Computations) ->
      List = [{concat_atom(Value, Computation), achlys_compute:compute(Computation, Value, Data, node())}
        || Computation <- Computations],
      analyse(T, Data, Round, Id, To, Acc ++ List);
    Computation when is_atom(Computation) ->
      analyse(T, Data, Round, Id, To, Acc ++ [{Computation, achlys_compute:compute(Computation, Data, node())}]);
    _ ->
      analyse(T, Data, Round, Id, To, Acc)
  end;

analyse([], _, Round, Id, To, Acc) ->
  io:fwrite("~p: ~p - ~p ~n", [Id, {Round, node(), Acc}, To]),
  lasp_update_temporary(Id, {Round, node(), Acc}, To).

lasp_update_temporary(Id, Values, forever) ->
  {ok, {C2, _, _, _}} = lasp:update(Id, {add, Values}, self()),
  C2;

lasp_update_temporary(Id, Values, Timeout) ->
  {ok, {C2, _, _, _}} = lasp:update(Id, {add, Values}, self()),
  spawn(fun() ->
    timer:sleep(Timeout),
    {ok, {_, _, _, _}} = lasp:update(C2, {rmv, Values}, self()),
    exit(terminated)
        end),
  C2.

get_variable_identifier(Name, Round) when is_atom(Name), is_integer(Round) ->
  unicode:characters_to_binary([erlang:atom_to_binary(Name, utf8), "_",
    list_to_binary(integer_to_list(Round))], utf8).

concat_atom(Name1, Name2) when is_atom(Name1), is_atom(Name2) ->
  binary_to_atom(unicode:characters_to_binary([erlang:atom_to_binary(Name1, utf8)
    , "_"
    , erlang:atom_to_binary(Name2, utf8)], utf8), utf8).

update_counter(GR, State) ->
  {ok, GCounter} = lasp:query(GR),
  if GCounter == State#state.round ->
    {ok, _} = lasp:update(GR, increment, self()),
    State#state.round + 1;
    true -> GCounter
  end.


wait_for_data(Cardinality, Id, Round, Timeout) ->
  spawn(fun() ->
    Self = self(),
    _Pid = spawn(fun() ->
      lasp:read(Id, {cardinality, Cardinality}),
      Self ! {self(), ok} end),
    receive
      {_Pid, ok} ->
        try_increase_cardinality(Cardinality, Id, Timeout),
        erlang:send(?SERVER, {analyse, Round, Id}),
        ok
    after
      Timeout ->
        erlang:send(?SERVER, {decrement_cardinality, Cardinality - 1}),
        erlang:send(?SERVER, {analyse, Round, Id}),
        ok
    end,
    exit(terminated)
        end),
  ok.

try_increase_cardinality(Cardinality, Id, Timeout) ->
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
      Timeout ->
        ok
    end,
    exit(terminated)
        end),
  ok.


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

%% @doc Returns the current pressure
%% if a Pmod_NAV module is active on slot SPI1
-spec maybe_get_press() -> {ok, [float()]} | pmod_nav_status().
maybe_get_press() ->
  {Code, Val} = is_pmod_nav_alive(),
  case {Code, Val} of
    {ok, pmod_nav} ->
      {ok, pmod_nav:read(alt, [press_out])};
    _ ->
      {Code, Val}
  end.

%% @doc Returns the current accelerometer values
%% if a Pmod_NAV module is active on slot SPI1
-spec maybe_get_acc() -> {ok, [float()]} | pmod_nav_status().
maybe_get_acc() ->
  {Code, Val} = is_pmod_nav_alive(),
  case {Code, Val} of
    {ok, pmod_nav} ->
      {ok, pmod_nav:read(acc, [out_x_xl, out_y_xl, out_z_xl])};
    _ ->
      {Code, Val}
  end.

%% @doc Returns the current gyroscope values
%% if a Pmod_NAV module is active on slot SPI1
-spec maybe_get_gyr() -> {ok, [float()]} | pmod_nav_status().
maybe_get_gyr() ->
  {Code, Val} = is_pmod_nav_alive(),
  case {Code, Val} of
    {ok, pmod_nav} ->
      {ok, pmod_nav:read(acc, [out_x_g, out_y_g, out_z_g])};
    _ ->
      {Code, Val}
  end.

%% @doc Returns the current magnetic field
%% if a Pmod_NAV module is active on slot SPI1
-spec maybe_get_mag() -> {ok, [float()]} | pmod_nav_status().
maybe_get_mag() ->
  {Code, Val} = is_pmod_nav_alive(),
  case {Code, Val} of
    {ok, pmod_nav} ->
      {ok, pmod_nav:read(mag, [out_x_m, out_y_m, out_z_m])};
    _ ->
      {Code, Val}
  end.

%% @doc Returns the current temperature
%% if a pmod_als module is active on slot SPI1
-spec maybe_get_light() -> {ok, 0..255} | pmod_als_status().
maybe_get_light() ->
  {Code, Val} = is_pmod_als_alive(),
  case {Code, Val} of
    {ok, pmod_als} ->
      {ok, pmod_als:read()};
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

%% @doc Checks the SPI2 slot of the GRiSP board
%% for presence of a Pmod_ALS module.
-spec is_pmod_als_alive() -> pmod_als_status().
is_pmod_als_alive() ->
  try grisp_devices:slot(?PMOD_ALS_SLOT) of
    {device, ?PMOD_ALS_SLOT, Device, _Pid, _Ref} when Device =:= pmod_als ->
      {ok, pmod_als};
    {device, ?PMOD_ALS_SLOT, Device, _Pid, _Ref} when Device =/= pmod_als ->
      {error, no_pmod_als}
  catch
    error:{no_device_connected, ?PMOD_ALS_SLOT} ->
      {error, no_device};
    _:_ ->
      {error, unknown}
  end.