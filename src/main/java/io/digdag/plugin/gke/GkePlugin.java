package io.digdag.plugin.gke;

import io.digdag.spi.OperatorFactory;
import io.digdag.spi.OperatorProvider;
import io.digdag.spi.Plugin;
import io.digdag.spi.TemplateEngine;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.spi.CommandExecutor;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

public class GkePlugin implements Plugin {
    @Override
    public <T> Class<? extends T> getServiceProvider(Class<T> type) {
        if (type == OperatorProvider.class) {
            return GkeOperatorProvider.class.asSubclass(type);
        } else {
            return null;
        }
    }

    public static class GkeOperatorProvider implements OperatorProvider {
        @Inject
        protected CommandExecutor exec;
        @Inject
        protected ObjectMapper mapper;

        @Override
        public List<OperatorFactory> get() {
            return Arrays.asList(new GkeOperatorFactory(exec, mapper));
        }
    }
}
