package org.analyzr.controller;

import org.analyzr.service.HelloService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author naimul.huda(mdnhudaATgmail.com)
 * @since 1/31/16
 */
@Controller
public class HelloController {

    @Autowired
    private HelloService helloService;

    @RequestMapping("/")
    public String index() {
        return "redirect:/greeting";
    }

    @RequestMapping("/greeting")
    @ResponseBody
    public String greeting() {
        return helloService.getGreeting();
    }

}