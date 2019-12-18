package io.digdag.plugin.gke;

import io.digdag.client.config.Config;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.spi.CommandExecutor;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableList;
import io.digdag.util.BaseOperator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskRequest;
import java.nio.file.Paths;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.hamcrest.Matchers.is;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GkeOperatorTest
{
    protected ObjectMapper mapper;
    protected CommandExecutor commandExecutor;
    protected OperatorContext operatorContext;

    @Before
    public void setUp()
            throws Exception
    {
        mapper = new ObjectMapper();
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

        final GkeOperatorFactory gkeOperatorFactory = new GkeOperatorFactory(this.commandExecutor, this.mapper);
        final GkeOperatorFactory.GkeOperator operator = gkeOperatorFactory.new GkeOperator(this.commandExecutor, this.mapper, this.operatorContext);

        Config childTaskRequestConfig = operator.generateChildTaskRequestConfig(testCluster, testParentConfig, testCommandConfig);


        String kubeConfigPath = System.getenv("KUBECONFIG");
        if (kubeConfigPath == null) {
          kubeConfigPath = Paths.get(System.getenv("HOME"), ".kube/config").toString();
        }

        Config desiredChildTaskRequestConfig = Config.deserializeFromJackson(this.mapper, this.mapper.createObjectNode())
          .set("_command", "echo test")
          .set("_type", "sh")
          .set("kubernetes", ImmutableMap.of("kube_config_path", kubeConfigPath, "cluster", testCluster));

        assertThat(childTaskRequestConfig, is(desiredChildTaskRequestConfig));
    }
}

