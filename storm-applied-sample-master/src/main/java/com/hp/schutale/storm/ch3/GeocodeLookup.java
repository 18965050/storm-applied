package com.hp.schutale.storm.ch3;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.code.geocoder.AdvancedGeoCoder;
import com.google.code.geocoder.Geocoder;
import com.google.code.geocoder.GeocoderRequestBuilder;
import com.google.code.geocoder.model.GeocodeResponse;
import com.google.code.geocoder.model.GeocoderAddressComponent;
import com.google.code.geocoder.model.GeocoderRequest;
import com.google.code.geocoder.model.GeocoderResult;
import com.google.code.geocoder.model.GeocoderStatus;
import com.google.code.geocoder.model.LatLng;

public class GeocodeLookup extends BaseBasicBolt {

    private Geocoder geocoder;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time-interval", "geocode", "city"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        HttpClient httpClient = new HttpClient(new MultiThreadedHttpConnectionManager());
        httpClient.getHostConfiguration().setProxy("web-proxy.corp.hp.com", 8088);
        geocoder = new AdvancedGeoCoder(httpClient);
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        String address = input.getStringByField("address");
        Long timeInterval = input.getLongByField("time-interval");

        GeocoderRequest request = new GeocoderRequestBuilder().setAddress(address).setLanguage("en")
                .getGeocoderRequest();

        try {
            GeocodeResponse response = geocoder.geocode(request);
            GeocoderStatus status = response.getStatus();
            if (GeocoderStatus.OK.equals(status)) {
                GeocoderResult firstResult = response.getResults().get(0);
                LatLng latLng = firstResult.getGeometry().getLocation();
                String city = extractCity(firstResult);
                collector.emit(new Values(timeInterval, latLng, city));
            } else {
                throw new IOException("Request returned with Status NOT OK: [" + status + "]");
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

    }

    private String extractCity(GeocoderResult result) {
        for (GeocoderAddressComponent component : result.getAddressComponents()) {
            if (component.getTypes().contains("locality")) {
                return component.getLongName();
            }
        }
        return "";
    }

}
