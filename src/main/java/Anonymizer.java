import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.server.Route;
import akka.pattern.Patterns;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CompletionStage;

import static akka.http.javadsl.server.Directives.*;

public class Anonymizer {
    public static Http http;
    public static ActorRef configStorageActor;
    private static final Duration timeout = Duration.ofSeconds(5);
    public static ZooKeeper keeper;
    private static int PORT;
    public static final String zookeeperConnectString  = "localhost:28015";

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        System.out.println("start!");
        PORT = Integer.parseInt(args[0]);
        ActorSystem system = ActorSystem.create("routes");
        configStorageActor = system.actorOf(Props.create(ConfigStorageActor.class));
        initZooKeeper();
        http = Http.get(system);
        final ActorMaterializer materializer =
                ActorMaterializer.create(system);
        final Flow<HttpRequest, HttpResponse, NotUsed> routeFlow =
                createRoute().flow(system, materializer);
        final CompletionStage<ServerBinding> binding = http.bindAndHandle(
                routeFlow,
                ConnectHttp.toHost("localhost", PORT),
                materializer
        );
        System.out.println("Server online\nPress RETURN to stop...");
        System.in.read();
        binding
                .thenCompose(ServerBinding::unbind)
                .thenAccept(unbound -> system.terminate());
    }

    private static CompletionStage<HttpResponse> fetch(String url) {
        return http.singleRequest(HttpRequest.create(url));
    }

    public static Route createRoute() {
        return route(get(() ->
            parameter("url", url ->
                    parameter("count", count -> {
                        if (Integer.parseInt(count) <= 0) {
                            return completeWithFuture(fetch(url));
                        }
                        return completeWithFuture(Patterns.ask(configStorageActor, new GetNextServer(), timeout)
                                .thenApply(nextPort -> (String)nextPort)
                                .thenCompose(nextPort -> fetch(String.format(
                                        "http://localhost:%s?url=%s&count=%d", nextPort, url, Integer.parseInt(count) - 1))));
                    }))));
    }

    public static Watcher watcher = watchedEvent -> {
        if (watchedEvent.getType() == Watcher.Event.EventType.NodeCreated ||
            watchedEvent.getType() == Watcher.Event.EventType.NodeDeleted ||
            watchedEvent.getType() == Watcher.Event.EventType.NodeDataChanged) {
            ArrayList<String> newServers = new ArrayList<>();
            try {
                for (String s: keeper.getChildren("/servers", false, null)) {
                    byte[] port = keeper.getData("/servers/" + s, false, null);
                    newServers.add(new String(port));
                }
                configStorageActor.tell(new RefreshServersMessage(newServers), ActorRef.noSender());
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    };

    public static void initZooKeeper() throws IOException, KeeperException, InterruptedException {
        keeper = new ZooKeeper(zookeeperConnectString, (int)timeout.getSeconds() * 1000, watcher);
        System.out.println("Creating server on port " + PORT);
        keeper.create("/servers/" + PORT, (PORT+"").getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        WatchedEvent event = new WatchedEvent(Watcher.Event.EventType.NodeCreated, Watcher.Event.KeeperState.SyncConnected, "");
        watcher.process(event);
    }
}
