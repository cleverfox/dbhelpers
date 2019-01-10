-module(dbapi).

-include("_build/default/lib/plob/include/plob.hrl").

-export([db2map/2,
         q/1,
         redis_hash_to_map/1,
%         geo2json/1,
         select/2,
         select_show/2,
         update/2,
         insert/2
        ]).

insert(Model, Object) when is_map(Object) ->
  #dbquery{sql=SQL, bindings=Bind}=plob:insert(Object,db_schema:Model()),
  pgapp:equery(SQL, Bind).

update(Model, Filter) when is_map(Filter) ->
  #dbquery{sql=SQL,bindings=Bindings}=plob:update(Filter,db_schema:Model()),
  pgapp:equery(SQL,Bindings).

select(Model, Filter) ->
  {SQL, Bind} = select0(Model, Filter),
  q(pgapp:equery(SQL,Bind)).

select_show(Model, Filter) ->
  {SQL, Bind} = select0(Model, Filter),
  {SQL, Bind}.

select0(Model, Filter) when is_map(Filter), is_atom(Model) ->
  #dbquery{sql=SQL,bindings=Bindings}=plob:find(Filter,db_schema:Model()),
  {SQL, Bindings};

select0(Model, Filter) when is_atom(Model) ->
  #dbquery{sql=SQL,bindings=Bindings}=plob:get(Filter,db_schema:Model()),
  {SQL, Bindings};

select0(SQL, Bindings) when is_list(SQL), is_list(Bindings) ->
  {SQL, Bindings}.

q({ok, Header, Body}) ->
  db2map(Header,Body);
q({error,_}=Any) ->
  Any.

db2map(Header,Payload) ->
  Header1=lists:map(fun({column,Title,_Type,_,_,_}) ->
                        binary_to_atom(Title,utf8);
                       ({column,Title,_Type,_,_,_,_}) ->
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

%geo2json(<<"MULTIPOINT(",Bin/binary>>) ->
%  Points=hd(binary:split(Bin,<<")">>)),
%  lists:map(fun(Pair) ->
%                [Bx,By]=binary:split(Pair,<<" ">>,[global]),
%                [
%                 try binary_to_float(Bx) catch error:badarg -> binary_to_integer(Bx) end, 
%                 try binary_to_float(By) catch error:badarg -> binary_to_integer(By) end
%                ]
%            end,
%            binary:split(Points,<<",">>,[global])
%           );
%
%geo2json(Bin) ->
%  Bin.
