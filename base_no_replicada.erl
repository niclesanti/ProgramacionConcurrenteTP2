%%% Módulo: base_no_replicada.erl
%%% Trabajo práctico: Base de datos no SQL Replicada
%%% Se implementa un servidor que mantiene un diccionario de pares clave/valor
%%% con cada valor acompañado de un timestamp y con manejo de “tombstones”.

-module(base_no_replicada).
-export([start/1, stop/0, put/3, remove/2, get/1, size/0]).
-export([servidor/1]).  % Exportamos el servidor para evitar warnings
%% Las funciones loop/1 y los handlers son privadas.

%%--------------------------------------------------------------------
%% API pública
%%--------------------------------------------------------------------

%% start/1: Inicia el proceso del servidor y lo registra con el nombre dado.
%% Si ya existe un proceso con ese nombre, falla.
start(Name) ->
    case whereis(Name) of
        undefined ->
            Pid = spawn(?MODULE, servidor, [#{}]),
            register(Name, Pid),
            erlang:put(diccionario_name, Name),
            {ok, Pid};
        _ ->
            {error, already_started}
    end.

%% stop/0: Detiene el proceso del servidor si está en ejecución.
stop() ->
    case erlang:get(diccionario_name) of
        undefined -> ok;
        Name ->
            case whereis(Name) of
                undefined -> ok;
                Pid ->
                    Pid ! stop,
                    unregister(Name),
                    erlang:erase(diccionario_name),
                    ok
            end
    end.

%% put/3: put(Key, Value, TimeStamp)
%% Inserta o actualiza la clave en el diccionario según la semántica de timestamp.
put(Key, Value, Ts) ->
    Name = erlang:get(diccionario_name),
    case whereis(Name) of
        undefined -> {error, no_server};
        Pid ->
            Pid ! {put, Key, Value, Ts, self()},
            receive Reply -> Reply end
    end.

%% remove/2: remove(Key, TimeStamp)
%% Elimina lógicamente la clave (colocando un tombstone) si el timestamp es mayor.
%% Cambié `rem/2` por `remove/2`
remove(Key, Ts) ->
    Name = erlang:get(diccionario_name),
    case whereis(Name) of
        undefined -> {error, no_server};
        Pid ->
            Pid ! {remove, Key, Ts, self()},
            receive Reply -> Reply end
    end.

%% get/1: get(Key)
%% Recupera la clave según la semántica descrita.
get(Key) ->
    Name = erlang:get(diccionario_name),
    case whereis(Name) of
        undefined -> {error, no_server};
        Pid ->
            Pid ! {get, Key, self()},
            receive Reply -> Reply end
    end.

%% size/0: Devuelve el número de claves activas en el diccionario.
size() ->
    Name = erlang:get(diccionario_name),
    case whereis(Name) of
        undefined -> {error, no_server};
        Pid ->
            Pid ! {size, self()},
            receive Reply -> Reply end
    end.

%%--------------------------------------------------------------------
%% Función principal del servidor
%% El estado es un mapa con entradas: Key => {Tipo, Valor, Ts}
%%--------------------------------------------------------------------
servidor(Dict) ->
    receive
        {put, Key, Value, Ts, From} ->
            {Reply, NewDict} = handle_put(Key, Value, Ts, Dict),
            From ! Reply,
            servidor(NewDict);
        
        {remove, Key, Ts, From} ->  %% Se cambio `rem` por `remove`
            {Reply, NewDict} = handle_remove(Key, Ts, Dict),
            From ! Reply,
            servidor(NewDict);
        
        {get, Key, From} ->
            Reply = handle_get(Key, Dict),
            From ! Reply,
            servidor(Dict);
        
        {size, From} ->
            Reply = handle_size(Dict),
            From ! Reply,
            servidor(Dict);
        
        stop ->
            exit(normal)
    end.

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

handle_size(Dict) ->
    maps:fold(fun(_Key, {value, _Value, _Ts}, Acc) -> Acc + 1;
                    (_Key, _Other, Acc) -> Acc
                end, 0, Dict).

