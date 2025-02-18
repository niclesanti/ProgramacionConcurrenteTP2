%%% Module: replicadb_test.erl
%%% Test EUnit para la base de datos NoSQL replicada (módulos base_de_datos y replica).

%%% INTEGRANTES:
%%% - Lazzarini Bautista.
%%% - Nicle Santiago.
%%% - Ramella Sebastian.

-module(replicadb_test).
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% Grupo de pruebas
%%--------------------------------------------------------------------
basic_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     {inorder, [
         fun start_replicas/0,
         fun put_one/0,
         fun put_quorum/0,
         fun put_all/0,
         fun remove_one/0,
         fun get_quorum/0,
         fun update/0,
         fun get_all/0,
         fun remove_quorum/0,
         fun remove_all/0,
         fun update_after_remove/0
     ]}
    }.

%%--------------------------------------------------------------------
%% Setup y Cleanup
%%--------------------------------------------------------------------
setup() ->
    %% Si la tabla ETS ya existe, se elimina para iniciar limpio.
    case ets:info(base_de_datos_replicas) of
        undefined -> ok;
        _ -> ets:delete(base_de_datos_replicas)
    end,
    ok = base_de_datos:start(miDB, 3),
    %% Espera breve para que todas las réplicas se inicien y registren.
    timer:sleep(50),
    ok.

cleanup(_) ->
    base_de_datos:stop(),
    case ets:info(base_de_datos_replicas) of
        undefined -> ok;
        _ -> ets:delete(base_de_datos_replicas)
    end.

%%--------------------------------------------------------------------
%% Pruebas
%%--------------------------------------------------------------------

%% Test 1: Verificar que se crean las réplicas correctamente.
start_replicas() ->
    [{replicas, ReplicaNames}] = ets:lookup(base_de_datos_replicas, replicas),
    Expected = ['miDB-1', 'miDB-2', 'miDB-3'],
    ?assertEqual(Expected, ReplicaNames).

%% Test 2: Operación PUT con consistencia one.
put_one() ->
    %% Se usa 'miDB-1' como coordinador.
    ?assertEqual(ok, replica:put(test_key, test_value, 100, one, 'miDB-1')),
    ?assertMatch({ok, test_value, 100}, replica:get(test_key, one, 'miDB-1')).

%% Test 3: Operación PUT con consistencia quorum.
put_quorum() ->
    %% Se usa 'miDB-2' como coordinador.
    ?assertEqual(ok, replica:put(q_key, q_value, 200, quorum, 'miDB-2')),
    ?assertMatch({ok, q_value, 200}, replica:get(q_key, quorum, 'miDB-2')).

%% Test 4: Operación PUT con consistencia all.
put_all() ->
    %% Se usa 'miDB-3' como coordinador.
    ?assertEqual(ok, replica:put(a_key, a_value, 300, all, 'miDB-3')),
    ?assertMatch({ok, a_value, 300}, replica:get(a_key, all, 'miDB-3')).

%% Test 5: Operación REMOVE con consistencia one.
remove_one() ->
    %% Inserta la clave y luego la elimina.
    ?assertEqual(ok, replica:put(r_key, r_value, 400, one, 'miDB-1')),
    ?assertMatch({ok, r_value, 400}, replica:get(r_key, one, 'miDB-1')),
    ?assertEqual(ok, replica:remove(r_key, 500, one, 'miDB-1')),
    ?assertMatch({ko, 500}, replica:get(r_key, one, 'miDB-1')).

%% Test 6: Operación GET con consistencia quorum.
get_quorum() ->
    %% Inserta la clave usando consistencia one y luego la consulta con quorum.
    ?assertEqual(ok, replica:put(g_key, g_value, 600, one, 'miDB-2')),
    ?assertMatch({ok, g_value, 600}, replica:get(g_key, quorum, 'miDB-2')).

%% Test 7: Actualización de clave (PUT) con timestamp.
update() ->
    %% Inserta la clave.
    ?assertEqual(ok, replica:put(u_key, initial, 700, one, 'miDB-1')),
    %% Actualizar con timestamp menor debe fallar.
    ?assertEqual(ko, replica:put(u_key, low_update, 650, one, 'miDB-1')),
    %% Actualizar con timestamp mayor debe tener efecto.
    ?assertEqual(ok, replica:put(u_key, high_update, 750, one, 'miDB-1')),
    ?assertMatch({ok, high_update, 750}, replica:get(u_key, one, 'miDB-1')).

%% Test 8: Operación GET con consistencia all.
get_all() ->
    %% Inserta la clave con consistencia one y luego la consulta con all.
    ?assertEqual(ok, replica:put(getall_key, getall_value, 800, one, 'miDB-3')),
    ?assertMatch({ok, getall_value, 800}, replica:get(getall_key, all, 'miDB-3')).

%% Test 9: Operación REMOVE con consistencia quorum.
remove_quorum() ->
    %% Inserta la clave y luego la elimina usando quorum.
    ?assertEqual(ok, replica:put(rq_key, rq_value, 900, one, 'miDB-1')),
    ?assertMatch({ok,rq_value,900}, replica:get(rq_key, one, 'miDB-1')),
    ?assertEqual(ok, replica:remove(rq_key, 1000, quorum, 'miDB-1')),
    ?assertMatch({ko, 1000}, replica:get(rq_key, one, 'miDB-1')).

%% Test 10: Operación REMOVE con consistencia all.
remove_all() ->
    %% Inserta la clave y luego la elimina usando all.
    ?assertEqual(ok, replica:put(ra_key, ra_value, 1100, one, 'miDB-2')),
    ?assertMatch({ok, ra_value, 1100}, replica:get(ra_key, one, 'miDB-2')),
    ?assertEqual(ok, replica:remove(ra_key, 1200, all, 'miDB-2')),
    ?assertMatch({ko, 1200}, replica:get(ra_key, one, 'miDB-2')).

%% Test 11: Actualización tras eliminación (uso de tombstone).
update_after_remove() ->
    %% Inserta la clave.
    ?assertEqual(ok, replica:put(uar_key, initial_value, 1300, one, 'miDB-3')),
    ?assertMatch({ok, initial_value, 1300}, replica:get(uar_key, one, 'miDB-3')),
    %% Elimina la clave.
    ?assertEqual(ok, replica:remove(uar_key, 1400, one, 'miDB-3')),
    ?assertMatch({ko, 1400}, replica:get(uar_key, one, 'miDB-3')),
    %% Intentar reinsertar con timestamp menor a la eliminación debe fallar.
    ?assertEqual(ko, replica:put(uar_key, new_value, 1350, one, 'miDB-3')),
    %% Reinsertar con timestamp mayor debe tener efecto.
    ?assertEqual(ok, replica:put(uar_key, new_value, 1450, one, 'miDB-3')),
    ?assertMatch({ok, new_value, 1450}, replica:get(uar_key, one, 'miDB-3')).
