package id.ac.soka.scheduling.realworld;

import com.fasterxml.jackson.databind.ObjectMapper;
import id.ac.soka.scheduling.algorithm.CsaScheduler;
import id.ac.soka.scheduling.algorithm.PhsScheduler;
import id.ac.soka.scheduling.data.GoCjRepository;
import id.ac.soka.scheduling.model.*;
import java.net.URI;
import java.net.http.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Real controller: the existing CSA/PHS creates the plan; HTTP workers execute its assignments. */
public final class RealWorldController {
    static final ObjectMapper JSON = new ObjectMapper();
    record TaskResult(int taskId,int vmId,String hostId,String status,String error,double arrivalSeconds,double dispatchSeconds,double startSeconds,double completionSeconds,double executionSeconds,double cpuSeconds) {}
    public static void main(String[] args) {
        try { execute(args); } catch(Exception e) { System.err.println("Real-world run failed: "+e.getMessage()); System.exit(2); }
    }
    static void execute(String[] args) throws Exception {
        String command=args[0], alg=opt(args,"--algorithm","CSA"), token=System.getenv("SOKA_WORKER_TOKEN");
        Path configPath=Path.of(opt(args,"--config","deploy/aws-workers.json")); ClusterConfig cluster=ClusterConfig.load(configPath);
        if(!"preflight".equals(command) && (token==null||token.isBlank())) throw new IllegalArgumentException("Set SOKA_WORKER_TOKEN before running");
        HttpClient client=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        preflight(cluster,client,token);
        if("preflight".equals(command)) { System.out.println("All configured workers passed preflight."); return; }
        String dataset=opt(args,"--dataset","dataset/GoCJ_Dataset_1000.csv"); int count=Integer.parseInt(opt(args,"--tasks","-1")); long seed=Long.parseLong(opt(args,"--seed","42"));
        List<TaskSpec> tasks=new GoCjRepository(Path.of(dataset).toAbsolutePath().getParent()).loadTasks(Path.of(dataset).getFileName().toString(),count<0?rows(Path.of(dataset)):count);
        List<VmSpec> vms=new ArrayList<>(); for(int i=0;i<cluster.workers().size();i++){var w=cluster.workers().get(i);vms.add(new VmSpec(i,0,i,"AWS",1000.0,w.slots(),1024,1000,10000));}
        AlgorithmOptions options=AlgorithmOptions.defaults();
        long scheduleStart=System.nanoTime(); Scheduler scheduler=switch(alg.toUpperCase(Locale.ROOT)){case "CSA"->new CsaScheduler(cluster.workers().size());case "PHS"->new PhsScheduler(cluster.workers().size());default->throw new IllegalArgumentException("algorithm must be CSA or PHS");};
        SchedulePlan plan=scheduler.schedule(tasks,vms,options,seed); double scheduleSeconds=(System.nanoTime()-scheduleStart)/1e9;
        Path out=Path.of(opt(args,"--output","results/"+alg.toLowerCase(Locale.ROOT)+"-run")); Files.createDirectories(out);
        String auth="Bearer "+token; long epoch=System.nanoTime(); ExecutorService[] pools=new ExecutorService[vms.size()]; for(int i=0;i<pools.length;i++)pools[i]=Executors.newFixedThreadPool(cluster.workers().get(i).slots());
        List<Future<TaskResult>> futures=new ArrayList<>(); int[] order=plan.taskOrder(), assignment=plan.assignedVm();
        for(int id:order){ if(id<0||id>=tasks.size())throw new IllegalStateException("invalid task id in plan: "+id); int vmId=assignment[id]; if(vmId<0||vmId>=pools.length)throw new IllegalStateException("invalid worker assignment: "+vmId); TaskSpec task=tasks.get(id); var worker=cluster.workers().get(vmId);
            futures.add(pools[vmId].submit(()->runTask(client,worker,vmId,task,auth,epoch)));
        }
        List<TaskResult> results=new ArrayList<>(); for(Future<TaskResult> f:futures)results.add(f.get()); for(var p:pools)p.shutdown();
        double makespan=results.stream().mapToDouble(TaskResult::completionSeconds).max().orElse(0); long succeeded=results.stream().filter(r->r.status().equals("SUCCEEDED")).count();
        double avgExec=results.stream().filter(r->r.status().equals("SUCCEEDED")).mapToDouble(TaskResult::executionSeconds).average().orElse(0);
        double avgWait=results.stream().mapToDouble(r->Math.max(0,r.startSeconds()-r.arrivalSeconds())).average().orElse(0);
        double avgTurn=results.stream().filter(r->r.status().equals("SUCCEEDED")).mapToDouble(r->r.completionSeconds()-r.arrivalSeconds()).average().orElse(0);
        double cost=0; for(int i=0;i<cluster.workers().size();i++)cost+=cluster.workers().get(i).modeledHourlyRateUsd()*makespan/3600.0;
        writeTasks(out.resolve("tasks.csv"),results); JSON.writerWithDefaultPrettyPrinter().writeValue(out.resolve("summary.json").toFile(),Map.of("algorithm",alg,"seed",seed,"taskCount",tasks.size(),"succeeded",succeeded,"failed",tasks.size()-succeeded,"makespanSeconds",makespan,"averageWaitingSeconds",avgWait,"averageExecutionSeconds",avgExec,"averageTurnaroundSeconds",avgTurn,"throughputTasksPerSecond",makespan>0?succeeded/makespan:0,"schedulingTimeSeconds",scheduleSeconds,"estimatedTotalCostUsd",cost,"resourceUtilizationEstimated",false,"parameters",options));
        JSON.writerWithDefaultPrettyPrinter().writeValue(out.resolve("config.json").toFile(),Map.of("cluster",cluster,"dataset",dataset,"algorithm",alg,"seed",seed));
        Files.writeString(out.resolve("metrics.csv"),"metric,value,kind\nmakespan,"+makespan+",measured\naverage_waiting,"+avgWait+",measured\naverage_execution,"+avgExec+",measured\naverage_turnaround,"+avgTurn+",measured\nthroughput,"+(makespan>0?succeeded/makespan:0)+",measured\nscheduling_time,"+scheduleSeconds+",measured\ntotal_cost_usd,"+cost+",estimated\n");
        writeWorkers(out.resolve("worker_metrics.csv"),cluster,results,makespan);
        System.out.println("Completed "+succeeded+"/"+tasks.size()+" tasks; results: "+out.toAbsolutePath());
    }
    static void preflight(ClusterConfig c,HttpClient client,String token)throws Exception { if(token==null||token.isBlank())throw new IllegalArgumentException("Set SOKA_WORKER_TOKEN for authenticated preflight"); for(var w:c.workers()){var req=HttpRequest.newBuilder(URI.create(trim(w.endpoint())+"/health")).timeout(Duration.ofSeconds(8)).header("Authorization","Bearer "+token).GET().build();var resp=client.send(req,HttpResponse.BodyHandlers.ofString()); if(resp.statusCode()!=200)throw new IllegalStateException("Health failed for "+w.endpoint()+": HTTP "+resp.statusCode()); var j=JSON.readTree(resp.body());if(!Objects.equals(j.path("hostId").asText(),w.hostId())||j.path("vmId").asInt(-1)!=w.vmId()||j.path("slots").asInt(-1)!=w.slots())throw new IllegalStateException("Identity/capacity mismatch for "+w.endpoint()+": "+resp.body()); System.out.println("ready "+w.hostId()+" vm="+w.vmId()+" slots="+w.slots());} }
    static TaskResult runTask(HttpClient c,ClusterConfig.Worker w,int vm,TaskSpec t,String auth,long epoch){double dispatch=elapsed(epoch); try{long wait=(long)(t.arrivalTime()*1000); if(wait>0)Thread.sleep(wait);double start=elapsed(epoch);var body=JSON.writeValueAsString(Map.of("taskId",t.id(),"lengthMi",t.lengthMi()));var req=HttpRequest.newBuilder(URI.create(trim(w.endpoint())+"/execute")).timeout(Duration.ofMinutes(10)).header("Authorization",auth).header("Content-Type","application/json").POST(HttpRequest.BodyPublishers.ofString(body)).build();var res=c.send(req,HttpResponse.BodyHandlers.ofString());double done=elapsed(epoch);if(res.statusCode()!=200)return new TaskResult(t.id(),vm,w.hostId(),"FAILED","HTTP "+res.statusCode()+": "+res.body(),t.arrivalTime(),dispatch,start,done,0,0);var j=JSON.readTree(res.body());return new TaskResult(t.id(),vm,w.hostId(),"SUCCEEDED","",t.arrivalTime(),dispatch,start,done,j.path("executionSeconds").asDouble(),j.path("cpuSeconds").asDouble());}catch(Exception e){double now=elapsed(epoch);return new TaskResult(t.id(),vm,w.hostId(),"FAILED",e.getClass().getSimpleName()+": "+e.getMessage(),t.arrivalTime(),dispatch,now,now,0,0);} }
    static int rows(Path p)throws Exception{return (int)Files.lines(p).filter(s->!s.isBlank()).count();} static double elapsed(long n){return (System.nanoTime()-n)/1e9;} static String trim(String s){return s.endsWith("/")?s.substring(0,s.length()-1):s;}
    static String opt(String[]a,String k,String d){for(int i=0;i<a.length-1;i++)if(a[i].equals(k))return a[i+1];return d;}
    static void writeTasks(Path p,List<TaskResult> r)throws Exception{try(var w=Files.newBufferedWriter(p)){w.write("task_id,vm_id,host_id,status,error,arrival_s,dispatch_s,start_s,completion_s,execution_s,cpu_s\n");for(var x:r)w.write(String.format(Locale.US,"%d,%d,%s,%s,\"%s\",%.6f,%.6f,%.6f,%.6f,%.6f,%.6f%n",x.taskId(),x.vmId(),x.hostId(),x.status(),x.error().replace("\"","\"\""),x.arrivalSeconds(),x.dispatchSeconds(),x.startSeconds(),x.completionSeconds(),x.executionSeconds(),x.cpuSeconds()));}}
    static void writeWorkers(Path p,ClusterConfig c,List<TaskResult> r,double makespan)throws Exception{try(var w=Files.newBufferedWriter(p)){w.write("host_id,vm_id,slots,assigned,succeeded,failed,estimated_utilization,modeled_hourly_rate_usd\n");for(int i=0;i<c.workers().size();i++){var x=c.workers().get(i);int vm=i;long assigned=r.stream().filter(z->z.vmId()==vm).count(),ok=r.stream().filter(z->z.vmId()==vm&&z.status().equals("SUCCEEDED")).count();double cpu=r.stream().filter(z->z.vmId()==vm).mapToDouble(TaskResult::cpuSeconds).sum(),util=makespan>0?Math.min(1,cpu/(makespan*x.slots())):0;w.write(String.format(Locale.US,"%s,%d,%d,%d,%d,%d,%.6f,%.6f%n",x.hostId(),x.vmId(),x.slots(),assigned,ok,assigned-ok,util,x.modeledHourlyRateUsd()));}}}
}
