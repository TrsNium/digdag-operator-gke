package io.digdag.plugin.gke;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskRequest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.file.Paths;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class GkeOperatorTest
{
    protected ObjectMapper mapper;
    protected ConfigFactory cf;
    protected CommandExecutor commandExecutor;
    protected OperatorContext operatorContext;

    @Before
    public void setUp()
            throws Exception
    {
        mapper = new ObjectMapper();
        cf = new ConfigFactory(mapper);
        commandExecutor = mock(CommandExecutor.class);
        operatorContext = mock(OperatorContext.class);
        TaskRequest taskRequest = mock(TaskRequest.class);

        Config taskRequestConfig = Config.deserializeFromJackson(this.mapper, this.mapper.createObjectNode());
        when(taskRequest.getConfig()).thenReturn(taskRequestConfig);

        when(operatorContext.getTaskRequest()).thenReturn(taskRequest);
        when(operatorContext.getProjectPath()).thenReturn(Paths.get("/testPath"));
    }

    @Test
    public void testGenerateChildTaskRequestConfig()
            throws Exception
    {
        final Config testCommandConfig = Config.deserializeFromJackson(this.mapper, this.mapper.createObjectNode()).set("sh>", "echo test");
        final Config testParentConfig = Config.deserializeFromJackson(this.mapper, this.mapper.createObjectNode()).set("_command", testCommandConfig).set("_type", "gke");
        final String testCluster = "test";

        final GkeOperatorFactory gkeOperatorFactory = new GkeOperatorFactory(this.commandExecutor, this.cf, this.mapper);
        final GkeOperatorFactory.GkeOperator operator = gkeOperatorFactory.new GkeOperator(this.commandExecutor, this.cf, this.mapper, this.operatorContext);

        final Config childTaskRequestConfig = operator.generateChildTaskRequestConfig(testCluster, testParentConfig, testCommandConfig);


        String kubeConfigPath = System.getenv("KUBECONFIG");
        if (kubeConfigPath == null) {
          kubeConfigPath = Paths.get(System.getenv("HOME"), ".kube/config").toString();
        }

        final Config desiredChildTaskRequestConfig = Config.deserializeFromJackson(this.mapper, this.mapper.createObjectNode())
          .set("_command", "echo test")
          .set("_type", "sh")
          .set("kubernetes", ImmutableMap.of("kube_config_path", kubeConfigPath, "name", testCluster));

        assertThat(childTaskRequestConfig, is(desiredChildTaskRequestConfig));
    }
}

