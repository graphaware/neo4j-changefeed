package com.graphaware.module.changefeed;

import org.neo4j.graphdb.GraphDatabaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Deque;
import java.util.List;

/**
 * Created by luanne on 02/07/14.
 */
@Controller
@RequestMapping("/changefeed")
public class ChangeFeedApi {

    private final ChangeFeed changeFeed;

    @Autowired
    public ChangeFeedApi(GraphDatabaseService database) {
        changeFeed=new ChangeFeed(database);

    }


    @RequestMapping(method = RequestMethod.GET)
    @ResponseBody
    public List<ChangeSet> getChangeFeed(@RequestParam(value = "since", required = false) Integer since) {
        return changeFeed.getChanges(since);
    }
}
