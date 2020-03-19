package org.agaveplatform.service.transfers.model;

public class SqlQuery {

    public static final String GET_ONE = "SELECT * FROM transfertasks WHERE \"uuid\" = ?";
    public static final String GET_PARENT = "SELECT * FROM transfertasks WHERE \"parentID\" = ?";
}
