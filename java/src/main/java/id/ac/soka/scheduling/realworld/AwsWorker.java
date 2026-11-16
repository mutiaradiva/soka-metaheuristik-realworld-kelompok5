package id.ac.soka.scheduling.realworld;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.lang.management.ManagementFactory;
import java.util.concurrent.*;

/** CPU workload service intended to run on each EC2 worker. */
public final class AwsWorker {
    record Request(int taskId, long lengthMi) {}
    record Result(int taskId, String hostId, int vmId, long lengthMi, double executionSeconds, double cpuSeconds, String status) {}
    static final ObjectMapper JSON = new ObjectMapper();
    public static void main(String[] args) throws Exception {
        String host = option(args,"--host-id",null), vm = option(args,"--vm-id",null), token = System.getenv("SOKA_WORKER_TOKEN");
        int slots = Integer.parseInt(option(args,"--slots","1")), port = Integer.parseInt(option(args,"--port","8080"));
        if (host == null || vm == null || token == null || token.isBlank()) throw new IllegalArgumentException("Set --host-id, --vm-id and SOKA_WORKER_TOKEN");
        int vmId = Integer.parseInt(vm); Semaphore capacity = new Semaphore(slots); String hostname = InetAddress.getLocalHost().getHostName();
        HttpServer server = HttpServer.create(new InetSocketAddress(option(args,"--bind","0.0.0.0"),port),0);
        server.setExecutor(Executors.newFixedThreadPool(slots+4));
        server.createContext("/health", ex -> { if (!auth(ex,token)) { send(ex,401,"{}"); return; } send(ex,200,JSON.writeValueAsString(java.util.Map.of("hostId",host,"vmId",vmId,"hostname",hostname,"slots",slots,"busySlots",slots-capacity.availablePermits()))); });
        server.createContext("/execute", ex -> {
            if (!"POST".equals(ex.getRequestMethod())) { send(ex,405,"{}"); return; }
            if (!auth(ex,token)) { send(ex,401,"{}"); return; }
            if (!capacity.tryAcquire()) { send(ex,429,"{\"error\":\"capacity full\"}"); return; }
            try {
                Request req=JSON.readValue(ex.getRequestBody(),Request.class); if(req.lengthMi()<=0) { send(ex,400,"{}"); return; }
                long start=System.nanoTime(), cpu0=ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime();
                // A bounded deterministic integer workload, scaled by MI and capped for safety.
                long iterations=Math.min(250_000_000L,Math.max(10_000L,req.lengthMi()*250L)); long x=0x9e3779b97f4a7c15L;
                for(long i=0;i<iterations;i++) x=(x^(x>>>12)^(x<<25)^(x>>>27))*0x2545F4914F6CDD1DL;
                double elapsed=(System.nanoTime()-start)/1e9, cpu=Math.max(0,(ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime()-cpu0)/1e9);
                send(ex,200,JSON.writeValueAsString(new Result(req.taskId(),host,vmId,req.lengthMi(),elapsed,cpu,"SUCCEEDED")));
            } catch(Exception e) { send(ex,500,"{\"status\":\"FAILED\",\"error\":\"execution error\"}"); }
            finally { capacity.release(); }
        });
        server.start(); System.out.println("Worker ready on port "+port+" host="+host+" vm="+vmId);
    }
    static String option(String[] a,String k,String d){ for(int i=0;i<a.length-1;i++) if(a[i].equals(k)) return a[i+1]; return d; }
    static boolean auth(HttpExchange ex,String t){ return ("Bearer "+t).equals(ex.getRequestHeaders().getFirst("Authorization")); }
    static void send(HttpExchange ex,int code,String body)throws java.io.IOException { byte[] b=body.getBytes(StandardCharsets.UTF_8); ex.getResponseHeaders().set("Content-Type","application/json"); ex.sendResponseHeaders(code,b.length); try(var o=ex.getResponseBody()){o.write(b);} }
}
