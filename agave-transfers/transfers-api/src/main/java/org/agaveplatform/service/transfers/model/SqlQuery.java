package org.agaveplatform.service.transfers.model;

public class SqlQuery {

    public static final String GET_ONE = "SELECT * FROM transfertasks WHERE \"uuid\" = ?";
    public static final String GET_PARENT = "SELECT * FROM transfertasks WHERE \"parentID\" = ?";
    public static final String ALL_CHILDREN_CANCELED_OR_COMPLETED = "select count(id) from transfertasks where (parentTask = {}) and status not in (('COMPLETED', 'CANCELLED','FAILED')";
    public static final String SET_TRANSFERTASK_CANCELLED_IF_NOT_COMPLETED = "UPDATE transfertasks set status = CANCELLED, lastUpdated = now() where uuid = {} and status not in ('COMPLETED', 'CANCELLED','FAILED')";
}
