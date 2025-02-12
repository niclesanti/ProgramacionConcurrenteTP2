%%% Módulo: base_no_replicada.erl
%%% Implementación de un diccionario en memoria con manejo de timestamps y operaciones concurrentes.

-module(base_no_replicada).
-export([start/1, stop/0, put/3, remove/2, get/1, size/0]).
-export([servidor/1]). % Para uso interno

%% Nombre de la tabla ETS para almacenar el nombre del servidor activo.
-define(SERVER_TABLE, base_no_replicada_server).

%%---------------------------------------------------------------------
%% API Pública
%%---------------------------------------------------------------------

%% start/1: Inicia el proceso del servidor y lo registra con el nombre dado.
%% Si ya existe un proceso con ese nombre, falla.
start(Name) ->
    %% Crear tabla ETS si no existe (almacena el nombre del servidor activo)
    ensure_ets_table(),
    case whereis(Name) of
        undefined ->
            Pid = spawn(?MODULE, servidor, [#{}]),
            register(Name, Pid),
            ets:insert(?SERVER_TABLE, {current_server, Name}),
            {ok, Pid};
        _ -> 
            {error, already_started}
    end.

%% stop/0: Detiene el servidor en ejecución y limpia el registro.
stop() ->
    case ets:lookup(?SERVER_TABLE, current_server) of
        [{current_server, Name}] ->
            case whereis(Name) of
                undefined -> ok;
                Pid ->
                    Pid ! stop,
                    unregister(Name),
                    ets:delete(?SERVER_TABLE, current_server)
            end;
        [] -> ok
    end.

%% put/3: put(Key, Value, TimeStamp)
%% Inserta o actualiza la clave en el diccionario según la semántica de timestamp.
%% Retorna ok si el cambio fue aplicado, ko en caso contrario.
put(Key, Value, Ts) ->
    case get_server_name() of
        {ok, Name} ->
            Pid = whereis(Name),
            Pid ! {put, Key, Value, Ts, self()},
            receive Reply -> Reply end;
        {error, Reason} -> {error, Reason}
    end.

%% remove/2: remove(Key, TimeStamp)
%% Elimina lógicamente la clave (colocando un tombstone) si el timestamp es mayor.
%% Retorna ok si se eliminó, notfound si no existía.
remove(Key, Ts) ->
    case get_server_name() of
        {ok, Name} ->
            Pid = whereis(Name),
            Pid ! {remove, Key, Ts, self()},
            receive Reply -> Reply end;
        {error, Reason} -> {error, Reason}
    end.

%% get/1: get(Key)
%% Obtiene el valor asociado a una clave.
%% Retorna {ok, Valor, Ts}, {ko, Ts} o notfound.
get(Key) ->
    case get_server_name() of
        {ok, Name} ->
            Pid = whereis(Name),
            Pid ! {get, Key, self()},
            receive Reply -> Reply end;
        {error, Reason} -> {error, Reason}
    end.

%% size/0: Retorna el número de claves activas en el diccionario.
size() ->
    case get_server_name() of
        {ok, Name} ->
            Pid = whereis(Name),
            Pid ! {size, self()},
            receive Reply -> Reply end;
        {error, Reason} -> {error, Reason}
    end.

%%---------------------------------------------------------------------
%% Funciones internas
%%---------------------------------------------------------------------

%% Crea la tabla ETS si no existe.
ensure_ets_table() ->
    case ets:info(?SERVER_TABLE) of
        undefined -> ets:new(?SERVER_TABLE, [named_table, public, set]);
        _ -> ok
    end.

%% Obtiene el nombre del servidor activo desde ETS.
get_server_name() ->
    case ets:lookup(?SERVER_TABLE, current_server) of
        [{current_server, Name}] -> {ok, Name};
        [] -> {error, no_server}
    end.

%%---------------------------------------------------------------------
%% Servidor principal
%%---------------------------------------------------------------------

%% Bucle principal del servidor. Maneja mensajes y mantiene el estado del diccionario.
servidor(Dict) ->
    receive
        %% Insertar/Actualizar clave
        {put, Key, Value, Ts, From} ->
            {Reply, NewDict} = handle_put(Key, Value, Ts, Dict),
            From ! Reply,
            servidor(NewDict);
        
        %% Eliminar clave
        {remove, Key, Ts, From} ->
            {Reply, NewDict} = handle_remove(Key, Ts, Dict),
            From ! Reply,
            servidor(NewDict);
        
        %% Obtener valor
        {get, Key, From} ->
            Reply = handle_get(Key, Dict),
            From ! Reply,
            servidor(Dict);
        
        %% Obtener tamaño
        {size, From} ->
            Reply = handle_size(Dict),
            From ! Reply,
            servidor(Dict);
        
        %% Detener servidor
        stop -> ok;
        
        %% Mensaje desconocido
        _ -> servidor(Dict)
    end.

%%---------------------------------------------------------------------
%% Handlers de operaciones
%%---------------------------------------------------------------------

%% Handler para operaciones PUT.
handle_put(Key, Value, Ts, Dict) ->
    case maps:find(Key, Dict) of
        {ok, {value, _, OldTs}} when Ts > OldTs ->
            {ok, maps:put(Key, {value, Value, Ts}, Dict)};
        {ok, {removed, OldTs}} when Ts > OldTs ->
            {ok, maps:put(Key, {value, Value, Ts}, Dict)};
        {ok, _} -> 
            {ko, Dict}; % Timestamp no es mayor
        error -> 
            {ok, maps:put(Key, {value, Value, Ts}, Dict)} % Nueva clave
    end.

%% Handler para operaciones REMOVE.
handle_remove(Key, Ts, Dict) ->
    case maps:find(Key, Dict) of
        {ok, {value, _, OldTs}} when Ts > OldTs ->
            {ok, maps:put(Key, {removed, Ts}, Dict)};
        {ok, {removed, OldTs}} when Ts > OldTs ->
            {notfound, maps:put(Key, {removed, Ts}, Dict)};
        {ok, _} -> 
            {notfound, Dict}; % Timestamp no es mayor
        error -> 
            {notfound, maps:put(Key, {removed, Ts}, Dict)} % Clave nueva marcada como eliminada
    end.

%% Handler para operaciones GET.
handle_get(Key, Dict) ->
    case maps:find(Key, Dict) of
        {ok, {value, Value, Ts}} -> {ok, Value, Ts};
        {ok, {removed, Ts}} -> {ko, Ts};
        error -> notfound
    end.

%% Handler para operaciones SIZE.
handle_size(Dict) ->
    maps:fold(fun(_, {value, _, _}, Acc) -> Acc + 1; (_, _, Acc) -> Acc end, 0, Dict).