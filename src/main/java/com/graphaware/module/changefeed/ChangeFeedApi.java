package com.graphaware.module.changefeed;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Deque;

/**
 * Created by luanne on 02/07/14.
 */
@Controller
@RequestMapping("/changefeed")
public class ChangeFeedApi {

    @RequestMapping(method = RequestMethod.GET)
    @ResponseBody
    public Deque<ChangeSet> getChangeFeed() {
        return ChangeFeedModule.getChanges();
    }
}
