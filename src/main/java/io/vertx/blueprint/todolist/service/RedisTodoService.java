package io.vertx.blueprint.todolist.service;

import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import io.vertx.blueprint.todolist.Constants;
import io.vertx.blueprint.todolist.entity.Todo;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.redis.client.Redis;
import io.vertx.reactivex.redis.client.RedisAPI;
import io.vertx.redis.client.RedisOptions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Redis implementation of {@link TodoService}.
 *
 * @author <a href="http://www.sczyh30.com">Eric Zhao</a>
 */
public class RedisTodoService implements TodoService {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisTodoService.class);
    private static  RedisAPI redis = null;

    private final Vertx vertx;
    private final RedisOptions options;

    public RedisTodoService(Vertx vertx, RedisOptions options) {
        this.vertx = vertx;
        this.options = options;
        redis = RedisAPI.api(Redis.createClient(vertx, options));
    }

    @Override
    public Completable initData() {
        Todo sample = new Todo(Math.abs(ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE)),
                "Something to do...", false, 1, "todo/ex");
        LOGGER.info("Seeding redis with [" + sample + "] " + sample.getId());
        return insert(sample)
                .ignoreElement();
    }

    @Override
    public Single<Todo> insert(Todo todo) {
        final String encoded = Json.encode(todo.toJson());
        return redis.
                rxHset(List.of(Constants.REDIS_TODO_KEY, String.valueOf(todo.getId()), encoded))
                .map(response -> todo)
                .toSingle();

    }

    @Override
    public Single<List<Todo>> getAll() {
        return redis.rxHvals(Constants.REDIS_TODO_KEY)
//                .map(response -> StreamSupport.stream(response.spliterator(), false)
//                        .map(Object::toString)
//                        .map(Todo::new)
//                        .collect(Collectors.toList())
//                ).toSingle(Collections.singletonList(new Todo("")));
    }

    @Override
    public Maybe<Todo> getCertain(String todoID) {
//        if (Objects.isNull(todoID)) {
//            return Maybe.empty();
//        }
//        return redis.rxHget(Constants.REDIS_TODO_KEY, todoID)
//                .map(response -> {
//                    Todo todo = new Todo(response.body);
//                    LOGGER.info("Response: " + todo);
//                    return todo;
//                });

    }

    @Override
    public Maybe<Todo> update(String key, Todo todo) {
        return getCertain(key)
                .map(value -> value.merge(todo))
                .flatMap(
                        event -> insert(event)
                                .flatMapMaybe(response -> Maybe.just(event))
                );
    }

    @Override
    public Completable delete(String todoId) {
        return redis.rxHdel(List.of(Constants.REDIS_TODO_KEY, todoId)).ignoreElement();
    }

    @Override
    public Completable deleteAll() {
        return redis.rxDel(List.of(Constants.REDIS_TODO_KEY)).ignoreElement();
    }
}
