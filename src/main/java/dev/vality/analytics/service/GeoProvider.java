package dev.vality.analytics.service;

import dev.vality.analytics.constant.ClickHouseUtilsValue;
import dev.vality.damsel.geo_ip.GeoIpServiceSrv;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class GeoProvider {

    private final GeoIpServiceSrv.Iface columbusClient;

    @Autowired
    public GeoProvider(GeoIpServiceSrv.Iface columbusClient) {
        this.columbusClient = columbusClient;
    }

    public String getLocationIsoCode(String ip) {
        try {
            return columbusClient.getLocationIsoCode(ip);
        } catch (Exception ex) {
            log.warn("Failed to get location info, ip='{}'", ip, ex);
            return ClickHouseUtilsValue.UNKNOWN;
        }
    }
}
