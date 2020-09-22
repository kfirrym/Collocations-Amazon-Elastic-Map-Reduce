import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import software.amazon.awssdk.services.ec2.model.InstanceType;

public class ExtractCollocations {

    public static void main(String[] args){
        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(Regions.US_EAST_1)
                .build();

        StepConfig enabledebugging = new StepConfig()
                .withName("Enable debugging")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(new StepFactory().newEnableDebuggingStep());

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("ExtractCollocations")
                .withReleaseLabel("emr-5.28.0")
                .withSteps(enabledebugging,
                        setStep(1, args[0]),
                        setStep(2, args[0]),
                        setStep(3, args[0]),
                        setStep(4, args[0]))
                .withLogUri("s3://echeb2/logs/")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withInstances(new JobFlowInstancesConfig()
                        .withEc2SubnetId("subnet-c59363e4")
                        .withInstanceCount(3)
                        .withKeepJobFlowAliveWhenNoSteps(false)
                        .withMasterInstanceType(InstanceType.M4_LARGE.toString())
                        .withSlaveInstanceType(InstanceType.M4_LARGE.toString()));

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        System.out.println("Ran job flow with id: " + runJobFlowResult.getJobFlowId());
    }

    private static StepConfig setStep(int stepCount, String lang){
        String[] steps = { "c", "Counter", "Recorder", "Recorder2", "RatioCalculator"};

        return new StepConfig()
                .withName(steps[stepCount])
                .withHadoopJarStep(new HadoopJarStepConfig()
                        .withJar("s3://orelkfir/" + steps[stepCount] + ".jar")
                        .withArgs("s3://echeb2/" + steps[stepCount - 1],
                                "s3://echeb2/" + steps[stepCount], lang))
                .withActionOnFailure("TERMINATE_JOB_FLOW");
    }
}
