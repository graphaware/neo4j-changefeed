package com.graphaware.module.changefeed;

import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;

/**
 * A set of changes applied on the graph in a single transaction
 */
public class ChangeSet {

    private int sequence;
    private Date changeDate;
    private Set<String> changes;


    public ChangeSet() {
        this.changeDate = new Date();
    }

    public Date getChangeDate() {
        return changeDate;
    }

    public void setChangeDate(Date changeDate) {
        this.changeDate = changeDate;
    }

    public Set<String> getChanges() {
        return changes;
    }

    public void setChanges(Set<String> changes) {
        this.changes = changes;
    }

    public String getChangeDateFormatted() {
        DateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
        return dateFormat.format(changeDate);
    }

    public int getSequence() {
        return sequence;
    }

    public void setSequence(int sequence) {
        this.sequence = sequence;
    }
}
