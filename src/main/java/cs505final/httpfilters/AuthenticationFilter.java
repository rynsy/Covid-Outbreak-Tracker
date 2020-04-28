package cs505final.httpfilters;


import cs505final.Launcher;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;


@Provider
public class AuthenticationFilter implements ContainerRequestFilter {


    /*
    * Not authenticating requests. Just keeping the file just in case I need it.
    *
    * */
    @Override
    public void filter(ContainerRequestContext requestContext) {}
}
