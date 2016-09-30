package org.analyzr.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * @author nhuda
 * @since 28/07/16
 */
@Service
public class HelloService {
    private Logger log = LoggerFactory.getLogger(getClass());

    public String getGreeting() {
        log.debug("Called getGreeting()");

        return "Hello Earthling!";
    }
}
