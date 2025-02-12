-module(base_de_datos).
-export([start/2, stop/0]).

%% Inicia N réplicas con nombres "Nombre-1", "Nombre-2", etc.
start(Nombre, N) ->
    ReplicaNames = generate_replica_names(Nombre, N),
    lists:foreach(
        fun(ReplicaName) ->
            OtherReplicas = lists:delete(ReplicaName, ReplicaNames),
            replica:start(ReplicaName, OtherReplicas)
        end,
        ReplicaNames
    ),
    ets:new(base_de_datos_replicas, [named_table, public, set]),
    ets:insert(base_de_datos_replicas, {replicas, ReplicaNames}),
    ok.

%% Detiene todas las réplicas.
stop() ->
    case ets:lookup(base_de_datos_replicas, replicas) of
        [{replicas, ReplicaNames}] ->
            lists:foreach(fun replica:stop/1, ReplicaNames),
            ets:delete(base_de_datos_replicas);
        _ -> ok
    end,
    ok.

%% Genera nombres de réplicas: "Nombre-1", "Nombre-2", etc.
generate_replica_names(Nombre, N) ->
    [list_to_atom(lists:concat([Nombre, "-", I])) || I <- lists:seq(1, N)].