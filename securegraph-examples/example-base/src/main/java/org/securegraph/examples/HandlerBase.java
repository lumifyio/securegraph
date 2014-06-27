package org.securegraph.examples;

import com.altamiracorp.miniweb.Handler;

import javax.servlet.http.HttpServletRequest;

public abstract class HandlerBase implements Handler {
    protected String getRequiredParameter(HttpServletRequest request, String name) {
        String val = request.getParameter(name);
        if (val == null) {
            throw new RuntimeException("Parameter " + name + " is required");
        }
        return val;
    }
}
