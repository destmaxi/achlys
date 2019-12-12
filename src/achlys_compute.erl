%%%-------------------------------------------------------------------
%%% @author bensim602
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 10. Dec 2019 16:08
%%%-------------------------------------------------------------------
-module(achlys_compute).
-author("bensim602").

%% API
-export([compute/4,
  compute/3]).

compute(Computation, Value, Data, Node) ->
  general_computation(Computation, retrieve(Data, Value), Node).


general_computation(average, Data, _) ->
  average(Data, 0, 0);

general_computation(standard_derivation, Data, _) ->
  Average = average(Data, 0, 0),
  case Average of
    X when is_number(X)-> standard_derivation(Data, X, 0, 0);
    {X, Y, Z} -> standard_derivation(Data, {X, Y, Z}, 0, 0);
    _ -> not_avalaible
  end;

general_computation(min, Data, _) ->
  min(Data, not_avalaible);

general_computation(max, Data, _) ->
  max(Data, not_avalaible);

general_computation(minX, Data, _) ->
  minX(Data, not_avalaible);

general_computation(maxX, Data, _) ->
  maxX(Data, not_avalaible);

general_computation(minY, Data, _) ->
  minY(Data, not_avalaible);

general_computation(maxY, Data, _) ->
  maxY(Data, not_avalaible);

general_computation(minZ, Data, _) ->
  minZ(Data, not_avalaible);

general_computation(maxZ, Data, _) ->
  maxZ(Data, not_avalaible);

general_computation(minXYZ, Data, _) ->
  minXYZ(Data, not_avalaible);

general_computation(maxXYZ, Data, _) ->
  maxXYZ(Data, not_avalaible);

general_computation(_, _, _) ->
  not_avalaible.



compute(temperature_warning, Data, Node) ->
  button_warning(retrieve(Data, temperature), Node, 1, -18, 40, []);

compute(pressure_warning, Data, Node) ->
  button_warning(retrieve(Data, pressure), Node, 2, 95000, 106000, []);

compute(_, _, _) ->
  not_avalaible.

%%====================================================================
%% Internal functions
%%====================================================================

retrieve(RawData, Values) when is_list(Values) ->
  [extract_data_from_node(X, Values) || X <- RawData];

retrieve(RawData, Values) when is_tuple(Values) ->
  [extract_data_from_node(X, erlang:tuple_to_list(Values)) || X <- RawData];

retrieve(RawData, Values) when is_atom(Values) ->
  [extract_data_from_node(X, [Values]) || X <- RawData].

extract_data_from_node({Node, NodeData}, Collect) ->
  extract_data_from_node(NodeData, Collect, {Node}).

extract_data_from_node(NodeData, [H | T], Acc) ->
  extract_data_from_node(NodeData, T, erlang:append_element(Acc, mapz:deep_get([H], NodeData, not_avalaible)));

extract_data_from_node(_, [], Acc) ->
  Acc.

average([{_, {X1, Y1, Z1}} | T], Sum, Len) ->
  case Sum of
    {X2, Y2, Z2} -> average(T, {X1 + X2, Y1 + Y2, Z1 + Z2}, Len + 1);
    _ -> average(T, {X1, Y1, Z1}, 1)
  end;
average([{_, X} | T], Sum, Len) when is_number(X) ->
  average(T, Sum + X, Len + 1);
average([_ | T], Sum, Len) ->
  average(T, Sum, Len);

average([], {X, Y, Z}, Len) when Len =/= 0 ->
  {X / Len, Y / Len, Z / Len};
average([], X, Len) when Len =/= 0, is_number(X) ->
  X / Len;
average(_, _, _) ->
  not_avalaible.

standard_derivation([{_, {X1, Y1, Z1}} | T], Average, Sum, Len) ->
  case Sum of
    {X2, Y2, Z2} -> standard_derivation(T, Average, {(X1 * X1) + X2, (Y1 * Y1) + Y2, (Z1 * Z1) + Z2}, Len + 1);
    _ -> standard_derivation(T, Average, {X1 * X1, Y1 * Y1, Z1 * Z1}, 1)
  end;
standard_derivation([{_, X} | T], Average, Sum, Len) when is_number(X) ->
  standard_derivation(T, Average, Sum + (X * X), Len + 1);
standard_derivation([_ | T], Average, Sum, Len) ->
  standard_derivation(T, Average, Sum, Len);

standard_derivation([], {AverageX, AverageY, AverageZ}, {X, Y, Z}, Len) when Len =/= 0 ->
  {math:sqrt((X / Len) - (AverageX * AverageX)), math:sqrt((Y / Len) - (AverageY * AverageY)), math:sqrt((Z / Len) - (AverageZ * AverageZ))};
standard_derivation([], Average, X, Len) when Len =/= 0, is_number(X) ->
  math:sqrt((X / Len) - (Average * Average));
standard_derivation(A, B, C, D) ->
  not_avalaible.

min([{Node1, {X1, Y1, Z1}} | T], Min) ->
  case Min of
    {Node2, {X2, Y2, Z2}} when X1 + Y1 + Z1 > X2 + Y2 + Z2 -> min(T, {Node2, {X2, Y2, Z2}});
    _ -> min(T, {Node1, {X1, Y1, Z1}})
  end;
min([{Node1, X1} | T], Min) when is_number(X1) ->
  case Min of
    {Node2, X2} when is_number(X2), X1 > X2 -> min(T, {Node2, X2});
    _ -> min(T, {Node1, X1})
  end;
min([_ | T], Min) ->
  min(T, Min);

min([], Min) ->
  Min.

max([{Node1, {X1, Y1, Z1}} | T], Max) ->
  case Max of
    {Node2, {X2, Y2, Z2}} when X1 + Y1 + Z1 < X2 + Y2 + Z2 -> max(T, {Node2, {X2, Y2, Z2}});
    _ -> max(T, {Node1, {X1, Y1, Z1}})
  end;
max([{Node1, X1} | T], Max) when is_number(X1) ->
  case Max of
    {Node2, X2} when is_number(X2), X1 < X2 -> max(T, {Node2, X2});
    _ -> max(T, {Node1, X1})
  end;
max([_ | T], Max) ->
  max(T, Max);

max([], Max) ->
  Max.

minX([{Node1, {X1, Y1, Z1}} | T], Min) ->
  case Min of
    {Node2, {X2, Y2, Z2}} when X1 > X2 -> minX(T, {Node2, {X2, Y2, Z2}});
    _ -> minX(T, {Node1, {X1, Y1, Z1}})
  end;
minX([{Node1, X1} | T], Min) when is_number(X1) ->
  not_avalaible;
minX([_ | T], Min) ->
  minX(T, Min);

minX([], Min) ->
  Min.

maxX([{Node1, {X1, Y1, Z1}} | T], Max) ->
  case Max of
    {Node2, {X2, Y2, Z2}} when X1 < X2 -> maxX(T, {Node2, {X2, Y2, Z2}});
    _ -> maxX(T, {Node1, {X1, Y1, Z1}})
  end;
maxX([{Node1, X1} | T], Max) when is_number(X1) ->
  not_avalaible;
maxX([_ | T], Max) ->
  maxX(T, Max);

maxX([], Max) ->
  Max.

minY([{Node1, {X1, Y1, Z1}} | T], Min) ->
  case Min of
    {Node2, {X2, Y2, Z2}} when Y1 > Y2 -> minY(T, {Node2, {X2, Y2, Z2}});
    _ -> minY(T, {Node1, {X1, Y1, Z1}})
  end;
minY([{Node1, X1} | T], Min) when is_number(X1) ->
  not_avalaible;
minY([_ | T], Min) ->
  minY(T, Min);

minY([], Min) ->
  Min.

maxY([{Node1, {X1, Y1, Z1}} | T], Max) ->
  case Max of
    {Node2, {X2, Y2, Z2}} when Y1 < Y2 -> maxY(T, {Node2, {X2, Y2, Z2}});
    _ -> maxY(T, {Node1, {X1, Y1, Z1}})
  end;
maxY([{Node1, X1} | T], Max) when is_number(X1) ->
  not_avalaible;
maxY([_ | T], Max) ->
  maxY(T, Max);

maxY([], Max) ->
  Max.

minZ([{Node1, {X1, Y1, Z1}} | T], Min) ->
  case Min of
    {Node2, {X2, Y2, Z2}} when Z1 > Z2 -> minZ(T, {Node2, {X2, Y2, Z2}});
    _ -> minZ(T, {Node1, {X1, Y1, Z1}})
  end;
minZ([{Node1, X1} | T], Min) when is_number(X1) ->
  not_avalaible;
minZ([_ | T], Min) ->
  minZ(T, Min);

minZ([], Min) ->
  Min.

maxZ([{Node1, {X1, Y1, Z1}} | T], Max) ->
  case Max of
    {Node2, {X2, Y2, Z2}} when Z1 < Z2 -> maxZ(T, {Node2, {X2, Y2, Z2}});
    _ -> maxZ(T, {Node1, {X1, Y1, Z1}})
  end;
maxZ([{Node1, X1} | T], Max) when is_number(X1) ->
  not_avalaible;
maxZ([_ | T], Max) ->
  maxZ(T, Max);

maxZ([], Max) ->
  Max.

minXYZ([{Node1, {X1, Y1, Z1}} | T], Min) ->
  case Min of
    {Node2, {X2, Y2, Z2}} ->
      case lists:min([X1, Y1, Z1]) > lists:min([X2, Y2, Z2]) of
        true -> minXYZ(T, {Node2, {X2, Y2, Z2}});
        _ -> minXYZ(T, {Node1, {X1, Y1, Z1}})
      end;
    _ -> minXYZ(T, {Node1, {X1, Y1, Z1}})
  end;
minXYZ([{Node1, X1} | T], Min) when is_number(X1) ->
  not_avalaible;
minXYZ([_ | T], Min) ->
  minXYZ(T, Min);

minXYZ([], Min) ->
  Min.

maxXYZ([{Node1, {X1, Y1, Z1}} | T], Max) ->
  case Max of
    {Node2, {X2, Y2, Z2}} ->
      case lists:max([X1, Y1, Z1]) < lists:max([X2, Y2, Z2]) of
        true -> maxXYZ(T, {Node2, {X2, Y2, Z2}});
        _ -> maxXYZ(T, {Node1, {X1, Y1, Z1}})
      end;
    _ -> maxXYZ(T, {Node1, {X1, Y1, Z1}})
  end;
maxXYZ([{Node1, X1} | T], Max) when is_number(X1) ->
  not_avalaible;
maxXYZ([_ | T], Max) ->
  maxXYZ(T, Max);

maxXYZ([], Max) ->
  Max.

button_warning([{Node, Value}|T], ThisNode, Led, Min, Max, Acc) when Value<Min; Value>Max->
  button_warning(T, ThisNode, Led, Min, Max, [Node|Acc]);
button_warning([_|T], ThisNode, Led, Min, Max, Acc) ->
  button_warning(T, ThisNode, Led, Min, Max, Acc);

button_warning([], _, Led, _, _, [])->
  grisp_led:color(Led, green),
  [];
button_warning([], ThisNode, Led, _, _, Acc)->
  case lists:member(ThisNode, Acc) of
    true -> grisp_led:color(Led, red);
    _ -> grisp_led:color(Led, blue)
  end,
  Acc.