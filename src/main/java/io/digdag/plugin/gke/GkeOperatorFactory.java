package io.digdag.plugin.gke;

import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import com.google.common.collect.ImmutableMap;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.ImmutableTaskRequest;
import io.digdag.spi.TemplateEngine;
import io.digdag.util.BaseOperator;
import io.digdag.util.BaseOperator;
import com.google.inject.Inject;
import java.io.IOException;

import java.util.Arrays;
import java.lang.Throwable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.ArrayList;

import io.digdag.standards.operator.ShOperatorFactory;
import io.digdag.standards.operator.RbOperatorFactory;
import io.digdag.standards.operator.PyOperatorFactory;
import com.google.common.base.Throwables;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.spi.CommandExecutor;

public class GkeOperatorFactory implements OperatorFactory {

    private final CommandExecutor exec;
    private final ObjectMapper mapper;

    public GkeOperatorFactory(CommandExecutor exec, ObjectMapper mapper) {
        this.exec = exec;
        this.mapper = mapper;
    }

    @Override
    public String getType() {
        return "gke";
    }

    @Override
    public Operator newOperator(OperatorContext context) {
        return new GkeOperator(this.exec, this.mapper, context);
    }

    private class GkeOperator extends BaseOperator {

        private final CommandExecutor exec;
        private final ObjectMapper mapper;
        GkeOperator(CommandExecutor exec, ObjectMapper mapper, OperatorContext context) {
            super(context);
            this.exec = exec;
            this.mapper = mapper;
        }

        @Override
        public TaskResult runTask() {
            Config params = request.getConfig().mergeDefault(
                request.getConfig().getNestedOrGetEmpty("gke"));


            String cluster = params.get("cluster", String.class);
            String project_id = params.get("project_id", String.class);
            String zone = params.get("zone", String.class);

            if (params.has("credential_json") || params.has("credential_json_path")) {
                authCLI(params);
            }

            // Auth GKECluster master with CLI
            List<String> authGkeCommandList = Arrays.asList("gcloud", "container", "clusters", "get-credentials", cluster, "--zone", zone, "--project", project_id);
            ProcessBuilder pb = new ProcessBuilder(authGkeCommandList);
            pb.inheritIO();
            try{
                final Process p = pb.start();
                p.waitFor();
            }
            catch (IOException | InterruptedException e) {
                throw Throwables.propagate(e);
            }

            TaskRequest childTaskRequest = generateChildTaskRequest(cluster, request, params);
            // gnerate OperatorContext via DefaultOperatorContext
            GkeOperatorContext internalOperatorContext = new GkeOperatorContext(
                    context.getProjectPath(),
                    childTaskRequest,
                    context.getSecrets(),
                    context.getPrivilegedVariables());

            Operator operator = null;
            switch (getChildCommandType(params)) {
                case "sh":
                    ShOperatorFactory shOperatorFactory = new ShOperatorFactory(exec);
                    operator = shOperatorFactory.newOperator(internalOperatorContext);
                    break;
                case "rb":
                    RbOperatorFactory rbOperatorFactory = new RbOperatorFactory(exec, mapper);
                    operator = rbOperatorFactory.newOperator(internalOperatorContext);
                    break;
                case "py":
                    PyOperatorFactory pyOperatorFactory = new PyOperatorFactory(exec, mapper);
                    operator = pyOperatorFactory.newOperator(internalOperatorContext);
                    break;
            }
            return operator.run();
        }

        private void authCLI(Config params) {
            String credentialJson = null;
            try {
                if (params.has("credential_json")){
                    credentialJson = params.get("credential_json", String.class);
                }
                else if (params.has("credential_json_path")){
                    String credentialPath = params.get("credential_json_path", String.class);
                    credentialJson = new String(Files.readAllBytes(Paths.get(credentialPath)));
                }
            }
            catch (IOException e) {
                throw new ConfigException("Please check gcp credential file and file path.");
            }

            String authCommand = String.format("echo << EOS %s EOS | openssl base64 -d -A | gcloud auth activate-service-account --key-file=-", credentialJson);
            List<String> authCommandList = Arrays.asList("/bin/bash", "-c", authCommand);
            ProcessBuilder pb = new ProcessBuilder(authCommandList);
            pb.inheritIO();
            try {
                final Process p = pb.start();
                p.waitFor();
            }
            catch (IOException | InterruptedException e) {
                throw Throwables.propagate(e);
            }
        }

        private String getChildCommandType(Config taskRequestConfig) {
            Config commandConfig = taskRequestConfig.getNestedOrGetEmpty("_command");
            String commandType = commandConfig.getKeys().get(0);
            return commandType.substring(0, commandType.length()-1);
        }

        private TaskRequest generateChildTaskRequest(String cluster, TaskRequest parentTaskRequest, Config parentTaskRequestConfig) {
            Config commandConfig = parentTaskRequestConfig.getNestedOrGetEmpty("_command");
            Config validatedCommandConfig = validateCommandConfig(commandConfig);
            Config childTaskRequestConfig = generateChildTaskRequestConfig(cluster, parentTaskRequestConfig, validatedCommandConfig);
            return ImmutableTaskRequest.builder()
                .siteId(parentTaskRequest.getSiteId())
                .projectId(parentTaskRequest.getProjectId())
                .workflowName(parentTaskRequest.getWorkflowName())
                .revision(parentTaskRequest.getRevision())
                .taskId(parentTaskRequest.getTaskId())
                .attemptId(parentTaskRequest.getAttemptId())
                .sessionId(parentTaskRequest.getSessionId())
                .taskName(parentTaskRequest.getTaskName())
                .lockId(parentTaskRequest.getLockId())
                .timeZone(parentTaskRequest.getTimeZone())
                .sessionUuid(parentTaskRequest.getSessionUuid())
                .sessionTime(parentTaskRequest.getSessionTime())
                .createdAt(parentTaskRequest.getCreatedAt())
                .config(childTaskRequestConfig)
                .localConfig(parentTaskRequest.getLocalConfig())
                .lastStateParams(parentTaskRequest.getLastStateParams())
                .build();
        }

        private Config validateCommandConfig(Config commandConfig) {
            if (commandConfig.getKeys().size() > 1) {
                throw new ConfigException("too many operator.");
            }

            String commandType = commandConfig.getKeys().get(0);
            if (! (commandType.equals("sh>") || commandType.equals("rb>") || commandType.equals("py>"))) {
                throw new ConfigException("GkeOperator support only sh>, rb> and py>.");
            }
            return commandConfig;
        }

        private Config generateChildTaskRequestConfig(String cluster, Config parentTaskRequestConfig, Config commandConfig){
            String commandType = commandConfig.getKeys().get(0);
            Config childTaskRequestConfig = parentTaskRequestConfig.deepCopy();
            childTaskRequestConfig.set("_command", commandConfig.get(commandType, String.class));
            // exclude > (end of commandType String).
            childTaskRequestConfig.set("_type", commandType.substring(0, commandType.length()-1));

            // set information for kubernetes command executor.
            String kubeConfigPath = System.getenv("KUBECONFIG");
            if (kubeConfigPath == null) {
                kubeConfigPath = Paths.get(System.getenv("HOME"), ".kube/config").toString();
            }

            if (childTaskRequestConfig.has("kuberntes")) {
                Config kubenretesConfig = childTaskRequestConfig.getNestedOrGetEmpty("kubernetes");
                childTaskRequestConfig.set("kube_config_path", kubeConfigPath);
                childTaskRequestConfig.set("cluster", cluster);
                childTaskRequestConfig.set("kubernetes", kubenretesConfig);
            } else {
                childTaskRequestConfig.set("kubernetes", ImmutableMap.of("kube_config_path", kubeConfigPath, "cluster", cluster));
            }
            return childTaskRequestConfig;
        }
    }
}
