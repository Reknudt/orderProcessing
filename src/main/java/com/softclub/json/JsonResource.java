package com.softclub.json;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.List;

@Path("/jsons")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class JsonResource {

    @Inject
    JsonProducer jsonProducer;
    @Inject
    JsonService jsonService;

    @GET
    public List<String> getJsonStreams() {
        List<String> json = jsonService.getJsons();
        if (json == null)
            throw new NotFoundException("Jsons not found");
        return json;
    }

    @GET
    @Path("/{id}")
    public String getJsonStreams(@PathParam("id") String id) {
        String json = jsonService.getJsonByKey(id);
        if (json == null)
            throw new NotFoundException("Json form not found by key: " + id);
        return json;
    }

    @POST
    public Response sendJson(String json) {
//        String key = jsonProducer.sendRawJsonScheme(json);
        String key = jsonProducer.sendValidatedJsonScheme(json);
        return Response.ok(key).build();
    }
}
