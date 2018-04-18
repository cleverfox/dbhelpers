-module(dbapi).

-export([db2map/2,
         redis_hash_to_map/1,
         geo2json/1
        ]).

db2map(Header,Payload) ->
  Header1=lists:map(fun({column,Title,_Type,_,_,_}) ->
                        binary_to_atom(Title,utf8)
                    end, Header),
  lists:map(fun(Item) ->
                maps:from_list(lists:zip(Header1, tuple_to_list(Item)))
            end, Payload).
redis_hash_to_map(D) ->
  redis_hash_to_map(D,#{}).
redis_hash_to_map([],Accumulated) -> Accumulated;
redis_hash_to_map([Key,Val|Rest],Accumulated) ->
  redis_hash_to_map(Rest,
                    maps:put(Key, Val, Accumulated)
                   ).

geo2json(<<"MULTIPOINT(",Bin/binary>>) ->
  Points=hd(binary:split(Bin,<<")">>)),
  lists:map(fun(Pair) ->
                [Bx,By]=binary:split(Pair,<<" ">>,[global]),
                [
                 try binary_to_float(Bx) catch error:badarg -> binary_to_integer(Bx) end, 
                 try binary_to_float(By) catch error:badarg -> binary_to_integer(By) end
                ]
            end,
            binary:split(Points,<<",">>,[global])
           );

geo2json(Bin) ->
  Bin.
