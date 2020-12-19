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
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

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

    public static void main(String[] args) throws IOException {
        System.out.println("start!");
        ActorSystem system = ActorSystem.create("routes");
        configStorageActor = system.actorOf(Props.create(ConfigStorageActor.class));
        final Http http = Http.get(system);
        final ActorMaterializer materializer =
                ActorMaterializer.create(system);
        final Flow<HttpRequest, HttpResponse, NotUsed> routeFlow =
                createRoute().flow(system, materializer);
        final CompletionStage<ServerBinding> binding = http.bindAndHandle(
                routeFlow,
                ConnectHttp.toHost("localhost", 8080),
                materializer
        );
        System.out.println("Server online at http://localhost:8080/\nPress RETURN to stop...");
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
                                .thenApply(nextServer -> (String)nextServer)
                                .thenCompose(nextServer -> fetch(nextServer + "&count=" + count)));
                    }))));
    }

    public static Watcher watcher = watchedEvent -> {
        if (watchedEvent.getType() == Watcher.Event.EventType.NodeCreated ||
            watchedEvent.getType() == Watcher.Event.EventType.NodeDeleted ||
            watchedEvent.getType() == Watcher.Event.EventType.NodeDataChanged) {
            ArrayList<String> newServers = new ArrayList<>();
            try {
                for (String s: keeper.getChildren("/servers", false, null)) {
                    byte[] url = keeper.getData("/servers/" + s, false, null);
                    newServers.add(new String(url));
                }
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    };

    public static void initZooKeeper() {
        
    }
}
