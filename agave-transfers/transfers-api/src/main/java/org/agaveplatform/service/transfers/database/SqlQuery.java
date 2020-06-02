/*
 *  Copyright (c) 2017 Red Hat, Inc. and/or its affiliates.
 *  Copyright (c) 2017 INSA Lyon, CITI Laboratory.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.agaveplatform.service.transfers.database;

/**
 * @author deardooley
 */
enum SqlQuery {
  CREATE_TRANSFERTASKS_TABLE,
  ALL_TRANSFERTASKS,
  ALL_USER_TRANSFERTASKS,
  GET_TRANSFERTASK,
  CREATE_TRANSFERTASK,
  SAVE_TRANSFERTASK,
  DELETE_TRANSFERTASK,
  UPDATE_TRANSFERTASK_STATUS,
  ALL_TRANSFERTASK_CHILDREN_CANCELLED_OR_COMPLETED,
  ALL_ACTIVE_ROOT_TRANSFERTASK_IDS,
  SINGLE_NOT_CANCELED_OR_COMPLETED,
  ALL_CHILDREN_CANCELED_OR_COMPLETED,
  SET_TRANSFERTASK_CANCELLED_WHERE_NOT_COMPLETED,
  GET_TRANSFERTASK_TREE
}
