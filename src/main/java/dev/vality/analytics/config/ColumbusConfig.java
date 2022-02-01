package dev.vality.analytics.config;

import dev.vality.damsel.geo_ip.GeoIpServiceSrv;
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
    public GeoIpServiceSrv.Iface columbusClient() throws IOException {
        return new THSpawnClientBuilder()
                .withAddress(resource.getURI()).build(GeoIpServiceSrv.Iface.class);
    }

}
