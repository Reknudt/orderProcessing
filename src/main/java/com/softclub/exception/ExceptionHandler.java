package com.softclub.exception;

import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

//@Provider
//public class ExceptionHandler implements ExceptionMapper<Throwable> {
//
//    @Override
//    public Response toResponse(Throwable e) {
//        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
//                .entity("An internal error occurred")
//                .build();
//    }
//}
