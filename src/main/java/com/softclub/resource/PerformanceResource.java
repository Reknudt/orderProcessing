package com.softclub.resource;

import com.softclub.service.PerformanceTestService;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import org.jboss.logging.Logger;


@Path("/performance")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class PerformanceResource {
    
    private static final Logger LOG = Logger.getLogger(PerformanceResource.class);
    
    @Inject
    PerformanceTestService performanceTestService;

    @POST
    @Path("/latencies")
    public void testLatencies(@QueryParam("countMessages") int countMessages) {
        LOG.infof("Проверка задержек: %s", countMessages);
        performanceTestService.runLatencyTest(countMessages);
    }

}