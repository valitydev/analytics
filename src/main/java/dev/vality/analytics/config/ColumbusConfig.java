package dev.vality.analytics.config;

import dev.vality.columbus.ColumbusServiceSrv;
import dev.vality.woody.thrift.impl.http.THSpawnClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import java.io.IOException;

@Configuration
public class ColumbusConfig {

    @Value("${columbus.url}")
    Resource resource;

    @Bean
    public ColumbusServiceSrv.Iface columbusClient() throws IOException {
        return new THSpawnClientBuilder()
                .withAddress(resource.getURI()).build(ColumbusServiceSrv.Iface.class);
    }

}
