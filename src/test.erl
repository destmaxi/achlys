%%%-------------------------------------------------------------------
%%% @author bensim602
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 17. Dec 2019 21:43
%%%-------------------------------------------------------------------
-module(test).
-author("bensim602").

%% API
-export([start/0]).

%%====================================================================
%% Macros
%%====================================================================

-define(SERVER, ?MODULE).
-define(PMOD_NAV_SLOT, spi1).
-define(PMOD_ALS_SLOT, spi2).
-define(TIME, erlang:monotonic_time()).

-record(state, {measures, global_round, crdt, table, counters}).

-type pmod_nav_status() :: {ok, pmod_nav}
| {error, no_device | no_pmod_nav | unknown}.

-type pmod_als_status() :: {ok, pmod_als}
| {error, no_device | no_pmod_als | unknown}.

-type coordinate() :: {number(), number(), number()}.

-type lasp_id() :: {binary(), atom()}.

start() ->
  MeasuresParameter = achlys_config:get(processing_worker, #{}),

  Ref = counters:new(2, [atomics]),
  counters:add(Ref, 2, mapz:deep_get([number_of_nodes], MeasuresParameter)),

  CounterID = {erlang:atom_to_binary(temperature_worker_counter, utf8), state_max_int},
  {ok, {Id, _, _, _}} = lasp:declare(CounterID, state_max_int),

  SetId = {erlang:atom_to_binary(analysed_data, utf8), state_gset},
  {ok, {Id2, _, _, _}} = lasp:declare(SetId, state_gset),

  T = [achlys_util:create_table(X) || X <- mapz:deep_get([collect], MeasuresParameter), is_atom(X)] ++
    [achlys_util:create_table(X) || {X, _, _} <- mapz:deep_get([collect], MeasuresParameter), is_atom(X)],

  State = #state{
    measures = MeasuresParameter,
    global_round = Id,
    crdt = Id2,
    table = T,
    counters = Ref},

  F = fun() -> test(State) end,
  achlys:bite(achlys:declare(aggregate, [node()], permanent, F)).

test(State) ->
  #{aggregation_trigger := A} = State#state.measures,
  C = State#state.counters,
  [poll_value(X) || X <- mapz:deep_get([collect], State#state.measures)],

  case lists:max([ets:info(X, size) || X <- State#state.table]) >= A of
    true ->
      aggregates(State),
      ok;
    false ->
      ok
  end.

aggregates(State) ->
  #{poll_interval := P
    , aggregation_trigger := A
    , timeout := To} = State#state.measures,
  GR = State#state.global_round,
  T = State#state.table,
  C = State#state.counters,
  Cardinality = counters:get(C, 2),
  update_counter(GR, C),
  NewRound = counters:get(C, 1),
  io:fwrite("Round ~p ~n", [NewRound]),

  Values = aggregate(T, A),

  SetId = {get_variable_identifier(processing_worker, NewRound), state_gset},
  {ok, {Id, _, _, _}} = lasp:declare(SetId, state_gset),
  C2 = lasp_update(Id, {node(), Values}),

  wait_for_data(Cardinality, C2, C, To),
  [ok = achlys_cleaner:flush_table(X) || X <- T].


%====================================================================
%% GenServer Internal functions
%%====================================================================

%% @doc Get the value from the sensor if possible and store it.
%% If a Min and Max values are specified, filter the value.
-spec poll_value(atom() | {atom(), number()|coordinate()}) -> ok.
poll_value(Value) when is_atom(Value) ->
  Res = read_sensor(Value),
  case Res of
    {ok, [Measure]} ->
      true = ets:insert_new(Value, {?TIME, Measure});
    {ok, [X, Y, Z]} ->
      true = ets:insert_new(Value, {?TIME, {X, Y, Z}});
    _ ->
      logger:log(notice, "Could not fetch data : ~p ~n", [Res])
  end,
  ok;
poll_value({Value, {XMin, YMin, ZMin}, {XMax, YMax, ZMax}}) when is_atom(Value) ->
  Res = read_sensor(Value),
  case Res of
    {ok, [X, Y, Z]} when X > XMin, Y > YMin, Z > ZMin, X < XMax, Y < YMax, Z < ZMax ->
      true = ets:insert_new(Value, {?TIME, {X, Y, Z}});
    _ ->
      logger:log(notice, "Could not fetch data : ~p ~n", [Res])
  end,
  ok;
poll_value({Value, Min, Max}) when is_atom(Value) ->
  Res = read_sensor(Value),
  case Res of
    {ok, [Measure]} when Measure > Min, Measure < Max ->
      true = ets:insert_new(Value, {?TIME, Measure});
    _ ->
      logger:log(notice, "Could not fetch data : ~p ~n", [Res])
  end,
  ok.

%% @doc Put all the mean of element in Values with enough values retrieved in a map
-spec aggregate(list(), pos_integer()) -> map().
aggregate(Values, A) -> aggregate(Values, A, #{}).

%% @doc function with acc for the function explained before
-spec aggregate(list(), pos_integer(), map()) -> map().
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
aggregate([], _, Acc) -> Acc.


%% @doc Update both my local counter and global counter (with Id GR) if up to date
%% or adjust local counter to the global one
-spec update_counter(lasp_id(), pos_integer()) -> pos_integer().
update_counter(GR, Counters) ->
  io:fwrite("yqsdgv: ~p - ~p ~n", [1234, 12345]),
  {ok, GCounter} = lasp:query(GR),
  LCounter = counters:get(Counters, 1),
  io:fwrite("yqsdgv: ~p - ~p ~n", [GCounter, LCounter]),
  if GCounter == LCounter ->
    {ok, _} = lasp:update(GR, increment, self()),
    counters:add(Counters, 1, 1);
    GCounter > LCounter-> counters:add(Counters, 1, GCounter-LCounter);
    true -> counters:sub(Counters, 1, LCounter-GCounter)
  end.

%% @doc Wait to receive cardinality data on Id.
%% If received before Timeout try to increase the cardinality,
%% otherwise send request to decrement cardinality.
%% In both case send request to analyse the data from this round.
-spec wait_for_data(pos_integer(), lasp_id(), any(), pos_integer()) -> ok.
wait_for_data(Cardinality, Id, Counters, Timeout) ->
  spawn(fun() ->
    Self = self(),
    _Pid = spawn(fun() ->
      lasp:read(Id, {cardinality, Cardinality}),
      Self ! {self(), ok} end),
    receive
      {_Pid, ok} ->
        try_increase_cardinality(Cardinality, Id, Counters, Timeout),
        io:fwrite("analyse ~p ~n", [counters:get(Counters, 2)]),
        ok
    after
      Timeout ->
        case Cardinality =:= counters:get(Counters, 2) of
          true -> counters:sub(Counters, 2, 1);
          _ -> ok
        end,
        io:fwrite("analyse ~p ~n", [counters:get(Counters, 2)]),
        ok
    end,
    exit(terminated)
        end),
  ok.

%% @doc Try to increment the cardinality by waiting for cardinality + 1 data
%% and send request to increment the cardinality if didn't timeout.
-spec try_increase_cardinality(pos_integer(), lasp_id(), any(), pos_integer()) -> ok.
try_increase_cardinality(Cardinality, Id, Counters, Timeout) ->
  spawn(fun() ->
    Self = self(),
    _Pid = spawn(fun() ->
      lasp:read(Id, {cardinality, Cardinality + 1}),
      Self ! {self(), ok} end),
    receive
      {_Pid, ok} ->
        case Cardinality =:= counters:get(Counters, 2) of
          true -> counters:add(Counters, 2, 1);
          _ -> ok
        end,
        ok
    after
      Timeout ->
        ok
    end,
    exit(terminated)
        end),
  ok.



%%====================================================================
%% Util functions
%%====================================================================

%% @doc Calculate the mean of the values in the list in ets with name Tab
%% The list is in format [{Time, Value}, ..]
-spec get_mean(atom()) -> number()|coordinate().
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

%% @doc Add the two element (element by element if coordinate)
-spec add(number()|coordinate(), number()|coordinate()) -> number()|coordinate().
add({X1, Y1, Z1}, {X2, Y2, Z2}) -> {X1 + X2, Y1 + Y2, Z1 + Z2};
add({X1, Y1, Z1}, 0) -> {X1, Y1, Z1};
add(A, B) -> A + B.


%%====================================================================
%% Lasp util functions
%%====================================================================

%% @doc Add the Value to the crdt with id Id and remove it after TimeOut ms
%% or never if Timeout is forever and return the Id
-spec lasp_update(lasp_id(), any()) -> lasp_id().
lasp_update(Id, Value) ->
  {ok, {C2, _, _, _}} = lasp:update(Id, {add, do_encode(Value)}, self()),
  C2.

lasp_read(Id) ->
  {ok, S} = lasp:query(Id),
  [do_decode(X) || X <-sets:to_list(S)].

%% @doc Returns the concatenation of the atom and the Round Number
%% with a "_" in between as a binary
-spec get_variable_identifier(atom(), pos_integer()) -> binary().
get_variable_identifier(Name, Round) when is_atom(Name), is_integer(Round) ->
  B1 = erlang:atom_to_binary(Name, utf8),
  B2 = <<"_">>,
  B3 = erlang:integer_to_binary(Round),
  <<B1/binary, B2/binary, B3/binary>>.

%% @doc Returns the concatenation of the 2 atoms with a "_" in between
-spec concat_atom(atom(), atom()) -> atom().
concat_atom(Name1, Name2) when is_atom(Name1), is_atom(Name2) ->
  list_to_atom(atom_to_list(Name1) ++ "_" ++ atom_to_list(Name2)).


-spec do_encode(Term :: term()) -> binary().
do_encode(Term) ->
  erlang:term_to_binary(Term, [{compressed, 9}]).

-spec do_decode(Bin :: binary()) -> term().
do_decode(Bin) ->
  erlang:binary_to_term(Bin).


%%====================================================================
%% Sensor reading functions
%%====================================================================

%% @doc Returns the current value of the sensor corresponding to Value
%% if a Pmod module is active
-spec read_sensor(atom()) -> {ok, [float()]} | pmod_nav_status() | pmod_als_status().
read_sensor(Value) ->
  if Value =:= temperature -> maybe_get_temp();
    Value =:= pressure -> maybe_get_press();
    Value =:= accelerometry -> maybe_get_acc();
    Value =:= gyroscopy -> maybe_get_gyr();
    Value =:= magnetic_field -> maybe_get_mag();
    Value =:= light -> maybe_get_light();
    true -> not_implemented
  end.

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