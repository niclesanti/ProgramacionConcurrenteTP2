-module(replica).
-export([start/2, stop/1, put/5, remove/4, get/3]).

%% Inicia una réplica con un nombre y lista de réplicas restantes.
start(Nombre, OtrasReplicas) ->
    Pid = spawn(fun() -> servidor(Nombre, OtrasReplicas, #{}) end),
    register(Nombre, Pid),
    ok.

%% Detiene una réplica.
stop(Nombre) ->
    case whereis(Nombre) of
        undefined -> ok;
        Pid -> Pid ! stop, ok
    end.

%% PUT: Clave, Valor, Timestamp, Consistencia, Coordinador.
put(Key, Value, Ts, Consistencia, Coordinador) ->
    Coordinador ! {put, Key, Value, Ts, Consistencia, self()},
    receive_response().

%% REMOVE: Clave, Timestamp, Consistencia, Coordinador.
remove(Key, Ts, Consistencia, Coordinador) ->
    Coordinador ! {remove, Key, Ts, Consistencia, self()},
    receive_response().

%% GET: Clave, Consistencia, Coordinador.
get(Key, Consistencia, Coordinador) ->
    Coordinador ! {get, Key, Consistencia, self()},
    receive_response().

%% Servidor principal de la réplica.
%% Servidor principal de la réplica (versión corregida).
servidor(Nombre, OtrasReplicas, Dict) ->
    receive
        %% ----------- PUT -----------
        %% Operación PUT con consistencia one (manejo síncrono)
        {put, Key, Value, Ts, one, From} ->
            {Reply, NewDict} = handle_put(Key, Value, Ts, Dict),
            From ! Reply,
            servidor(Nombre, OtrasReplicas, NewDict);

        %% Operación PUT con quorum/all (manejo asíncrono)
        {put, Key, Value, Ts, Consistencia, From} ->
            spawn(fun() -> 
                handle_coordinacion_put(Key, Value, Ts, Consistencia, Nombre, OtrasReplicas, From, Dict) 
            end),
            servidor(Nombre, OtrasReplicas, Dict);

        %% ----------- REMOVE -----------
        %% Operación REMOVE con consistencia one (manejo síncrono)
        {remove, Key, Ts, one, From} ->
            {Reply, NewDict} = handle_remove(Key, Ts, Dict),
            From ! Reply,
            servidor(Nombre, OtrasReplicas, NewDict);

        %% Operación REMOVE con quorum/all (manejo asíncrono)
        {remove, Key, Ts, Consistencia, From} ->
            spawn(fun() -> 
                handle_coordinacion_remove(Key, Ts, Consistencia, Nombre, OtrasReplicas, From, Dict) 
            end),
            servidor(Nombre, OtrasReplicas, Dict);

        %% ----------- GET -----------
        %% Operación GET con consistencia one (manejo síncrono)
        {get, Key, one, From} ->
            Reply = handle_get(Key, Dict),
            From ! Reply,
            servidor(Nombre, OtrasReplicas, Dict);

        %% Operación GET con quorum/all (manejo asíncrono)
        {get, Key, Consistencia, From} ->
            spawn(fun() -> 
                handle_coordinacion_get(Key, Consistencia, Nombre, OtrasReplicas, From, Dict) 
            end),
            servidor(Nombre, OtrasReplicas, Dict);

        %% ----------- MENSAJES DE RÉPLICAS -----------
        {put_request, Key, Value, Ts, From} ->
            {Reply, NewDict} = handle_put(Key, Value, Ts, Dict),
            From ! {put_response, Reply},
            servidor(Nombre, OtrasReplicas, NewDict);

        {remove_request, Key, Ts, From} ->
            {Reply, NewDict} = handle_remove(Key, Ts, Dict),
            From ! {remove_response, Reply},
            servidor(Nombre, OtrasReplicas, NewDict);

        {get_request, Key, From} ->
            Reply = handle_get(Key, Dict),
            From ! {get_response, Reply},
            servidor(Nombre, OtrasReplicas, Dict);

        %% ----------- STOP -----------
        stop -> ok;

        %% ----------- MENSAJES DESCONOCIDOS -----------
        _ -> servidor(Nombre, OtrasReplicas, Dict)
    end.

%% -------------------------
%% Handlers para coordinación
%% -------------------------

%% PUT: Coordinación según nivel de consistencia.
handle_coordinacion_put(Key, Value, Ts, Consistencia, Coordinador, OtrasReplicas, From, Dict) ->
    AllReplicas = [Coordinador | OtrasReplicas],
    case Consistencia of
        one ->
            {Reply, _} = handle_put(Key, Value, Ts, Dict),
            From ! Reply;
        _ ->
            send_to_replicas(put_request, Key, Value, Ts, AllReplicas),
            Required = case Consistencia of
                quorum -> (length(AllReplicas) div 2 + 1);
                all -> length(AllReplicas)
            end,
            Responses = collect_responses(put_response, length(AllReplicas), Required),
            Oks = [R || R <- Responses, R =:= ok],
            if
                length(Oks) >= Required -> From ! ok;
                true -> From ! ko
            end
    end.

%% REMOVE: Coordinación según nivel de consistencia.
handle_coordinacion_remove(Key, Ts, Consistencia, Coordinador, OtrasReplicas, From, Dict) ->
    AllReplicas = [Coordinador | OtrasReplicas],
    case Consistencia of
        one ->
            {Reply, _} = handle_remove(Key, Ts, Dict),
            From ! Reply;
        _ ->
            send_to_replicas(remove_request, Key, Ts, AllReplicas),
            Required = case Consistencia of
                quorum -> (length(AllReplicas) div 2 + 1);
                all -> length(AllReplicas)
            end,
            Responses = collect_responses(remove_response, length(AllReplicas), Required),
            Oks = [R || R <- Responses, R =:= ok],
            if
                length(Oks) >= Required -> From ! ok;
                true -> From ! ko
            end
    end.

%% GET: Coordinación según nivel de consistencia.
handle_coordinacion_get(Key, Consistencia, Coordinador, OtrasReplicas, From, Dict) ->
    AllReplicas = [Coordinador | OtrasReplicas],
    case Consistencia of
        one ->
            Reply = handle_get(Key, Dict),
            From ! Reply;
        _ ->
            send_to_replicas(get_request, Key, AllReplicas),
            Required = case Consistencia of
                quorum -> (length(AllReplicas) div 2 + 1);
                all -> length(AllReplicas)
            end,
            Responses = collect_responses(get_response, length(AllReplicas), Required),
            filter_max_timestamp(Responses, From)
    end.

%% -------------------------
%% Funciones auxiliares
%% -------------------------

%% Envía una solicitud a todas las réplicas (usada para PUT y REMOVE).
send_to_replicas(Type, Key, Value, Ts, Replicas) ->
    [R ! {Type, Key, Value, Ts, self()} || R <- Replicas].

%% Envía una solicitud GET a todas las réplicas.
send_to_replicas(get_request, Key, Replicas) ->
    [R ! {get_request, Key, self()} || R <- Replicas].

%% Envía una solicitud de REMOVE a todas las réplicas.
send_to_replicas(remove_request, Key, Ts, Replicas) ->
    [R ! {remove_request, Key, Ts, self()} || R <- Replicas].

%% Colecciona respuestas hasta alcanzar el número requerido.
collect_responses(Type, Total, Required) ->
    collect_responses(Type, Total, Required, []).

collect_responses(_, 0, _, Acc) -> Acc;
collect_responses(Type, Remaining, Required, Acc) ->
    receive
        {Type, Reply} ->
            NewAcc = [Reply | Acc],
            if
                length(NewAcc) >= Required -> NewAcc;
                true -> collect_responses(Type, Remaining - 1, Required, NewAcc)
            end
    after 5000 -> Acc
    end.

%% Filtra y selecciona la respuesta con el mayor timestamp para GET.
filter_max_timestamp(Responses, From) ->
    ValidResponses = lists:filter(fun(R) -> R =/= notfound end, Responses),
    case ValidResponses of
        [] -> From ! notfound;
        _ ->
            MaxResponse = lists:foldl(
                fun(R, Acc) ->
                    TsR = get_timestamp(R),
                    TsAcc = get_timestamp(Acc),
                    case TsR > TsAcc of
                        true -> R;
                        false -> Acc
                    end
                end,
                hd(ValidResponses),
                ValidResponses
            ),
            From ! MaxResponse
    end.


get_timestamp({ok, _, Ts}) -> Ts;
get_timestamp({ko, Ts}) -> Ts.


%%--------------------------------------------------------------------
%% Handlers para las operaciones
%%--------------------------------------------------------------------

handle_put(Key, Value, Ts, Dict) ->
    case maps:get(Key, Dict, undefined) of
        undefined ->
            {ok, maps:put(Key, {value, Value, Ts}, Dict)};
        {value, _OldValue, OldTs} ->
            if Ts > OldTs -> {ok, maps:put(Key, {value, Value, Ts}, Dict)};
               true -> {ko, Dict}
            end;
        {removed, OldTs} ->
            if Ts > OldTs -> {ok, maps:put(Key, {value, Value, Ts}, Dict)};
               true -> {ko, Dict}
            end
    end.

handle_remove(Key, Ts, Dict) ->  
    case maps:get(Key, Dict, undefined) of
        undefined ->
            {notfound, maps:put(Key, {removed, Ts}, Dict)};
        {value, _OldValue, OldTs} ->
            if Ts > OldTs -> {ok, maps:put(Key, {removed, Ts}, Dict)};
                true -> {notfound, Dict}
            end;
        {removed, OldTs} ->
            if Ts > OldTs -> {notfound, maps:put(Key, {removed, Ts}, Dict)};
                true -> {notfound, Dict}
            end
    end.

handle_get(Key, Dict) ->
    case maps:get(Key, Dict, undefined) of
        undefined -> notfound;
        {value, Value, Ts} -> {ok, Value, Ts};
        {removed, Ts} -> {ko, Ts}
    end.

%% Espera y retorna la respuesta del coordinador (con timeout).
receive_response() ->
    receive
        Reply -> Reply
    after 5000 ->
        {error, timeout}
    end.
