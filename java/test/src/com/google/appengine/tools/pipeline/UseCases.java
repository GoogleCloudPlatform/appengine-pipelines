// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.pipeline;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;

import java.util.LinkedList;
import java.util.List;

/**
 * @author rudominer@google.com (Your Name Here)
 *
 */
public class UseCases {

  public class GenerateReportJob extends Job0<Void> {
    @Override
    public Value<Void> run() {
      FutureValue<Void> customerMap = futureCall(new MapDatastoreJob());
      FutureValue<List<CustomerReport>> customerReportList =
          futureCall(new ReportsForMarkJob(), waitFor(customerMap));
      futureCall(new MailReportJob(), customerReportList);
      return null;
    }
  }

  public class CustomerReport {
  }

  public class MailReportJob extends Job1<Void, List<CustomerReport>> {
    @Override
    public Value<Void> run(List<CustomerReport> customerReportList) {
      // Mail reports
      return null;
    }
  }

  public class MapDatastoreJob extends Job0<Void> {
    @Override
    public Value<Void> run() {
      // Do the mapping over the DataStore
      return null;
    }
  }

  public class ReportsForMarkJob extends Job0<List<CustomerReport>> {
    @Override
    public Value<List<CustomerReport>> run() {
      DatastoreService dataStore = DatastoreServiceFactory.getDatastoreService();
      Query query = new Query();
      query.addFilter("somProperty", Query.FilterOperator.EQUAL, "someValue");
      PreparedQuery preparedQuery = dataStore.prepare(query);
      List<FutureValue<CustomerReport>> listOfFutureValues =
          new LinkedList<FutureValue<CustomerReport>>();
      for (Entity entity : preparedQuery.asIterable()) {
        int customerId = (Integer) entity.getProperty("customerId");
        listOfFutureValues.add(futureCall(new ComputeCustomerMetricJob(), immediate(customerId)));
      }
      return futureList(listOfFutureValues);
    }
  }

  // Computes and returns a customer report given a customer Id
  public class ComputeCustomerMetricJob extends Job1<CustomerReport, Integer> {
    @Override
    public Value<CustomerReport> run(Integer customerId) {
      return immediate(new CustomerReport());
    }
  }
}
