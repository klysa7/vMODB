package dk.ku.di.dms.vms.tpcc.order.infra;

import dk.ku.di.dms.vms.modb.api.query.builder.QueryBuilderFactory;
import dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.sdk.embed.client.DefaultHttpHandler;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;
import dk.ku.di.dms.vms.tpcc.common.datagen.TPCcConstants;
import dk.ku.di.dms.vms.tpcc.order.HATtrickCounters;
import dk.ku.di.dms.vms.tpcc.order.entities.Freshness;
import dk.ku.di.dms.vms.tpcc.order.entities.NewOrder;
import dk.ku.di.dms.vms.tpcc.order.entities.Order;
import dk.ku.di.dms.vms.tpcc.order.entities.OrderLine;
import dk.ku.di.dms.vms.tpcc.order.repositories.IFreshnessRepository;
import dk.ku.di.dms.vms.tpcc.order.repositories.IHistoryRepository;
import dk.ku.di.dms.vms.tpcc.order.repositories.INewOrderRepository;
import dk.ku.di.dms.vms.tpcc.order.repositories.IOrderLineRepository;
import dk.ku.di.dms.vms.tpcc.order.repositories.IOrderRepository;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import static dk.ku.di.dms.vms.tpcc.common.datagen.DataGenUtils.makeAlphaString;
import static dk.ku.di.dms.vms.tpcc.common.datagen.DataGenUtils.randomNumber;
import static java.lang.System.Logger.Level.*;

public final class OrderHttpHandler extends DefaultHttpHandler {

    private final IOrderRepository     orderRepository;
    private final INewOrderRepository  newOrderRepository;
    private final IOrderLineRepository orderLineRepository;
    private final IHistoryRepository   historyRepository;

    // ── HATtrick addition ────────────────────────────────────────────────────
    private final IFreshnessRepository freshnessRepository;
    private int numTClients = 2; // set from properties at PUT /; default 2
    // ─────────────────────────────────────────────────────────────────────────

    public OrderHttpHandler(ITransactionManager  transactionManager,
                            IOrderRepository     orderRepository,
                            INewOrderRepository  newOrderRepository,
                            IOrderLineRepository orderLineRepository,
                            IHistoryRepository   historyRepository,
                            IFreshnessRepository freshnessRepository) {
        super(transactionManager);
        this.orderRepository     = orderRepository;
        this.newOrderRepository  = newOrderRepository;
        this.orderLineRepository = orderLineRepository;
        this.historyRepository   = historyRepository;
        this.freshnessRepository = freshnessRepository;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Query statements
    // ─────────────────────────────────────────────────────────────────────────

    // Legacy check queries (unchanged)
    private static final SelectStatement selectStatementOrder =
            QueryBuilderFactory.select().project("*").from("orders")
                    .where("o_id", ExpressionTypeEnum.LESS_THAN_OR_EQUAL, 3000).build();

    private static final SelectStatement selectStatementNewOrder =
            QueryBuilderFactory.select().project("*").from("new_orders")
                    .where("no_o_id", ExpressionTypeEnum.LESS_THAN_OR_EQUAL, 3000).build();

    private static final SelectStatement selectStatementOrderLine =
            QueryBuilderFactory.select().project("*").from("order_line")
                    .where("ol_o_id", ExpressionTypeEnum.LESS_THAN_OR_EQUAL, 3000).build();

    // ── HATtrick: full-table count SELECT statements ──────────────────────────
    private static final SelectStatement selectAllOrders =
            QueryBuilderFactory.select().project("*").from("orders").build();

    private static final SelectStatement selectAllOrderLine =
            QueryBuilderFactory.select().project("*").from("order_line").build();

    private static final SelectStatement selectAllHistory =
            QueryBuilderFactory.select().project("*").from("history").build();
    // ─────────────────────────────────────────────────────────────────────────

    // ─────────────────────────────────────────────────────────────────────────
    // PATCH — cleanup / reset (unchanged)
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public void patch(String uri, String body) {
        final String[] uriSplit = uri.split("/");
        String op = uriSplit[uriSplit.length - 1];
        if (op.contentEquals("reset")) {
            this.transactionManager.reset();
            return;
        }
        LOGGER.log(INFO, "Order init cleanup");

        this.transactionManager.beginTransaction(Long.MAX_VALUE, 0, 0, false);
        List<Order>     orders     = this.orderRepository.query(selectStatementOrder);
        List<NewOrder>  newOrders  = this.newOrderRepository.query(selectStatementNewOrder);
        List<OrderLine> orderLines = this.orderLineRepository.query(selectStatementOrderLine);
        this.transactionManager.reset();

        LOGGER.log(INFO, "Order GC triggered.");
        System.gc();
        LOGGER.log(INFO, "Order GC finished.");

        this.transactionManager.beginTransaction(0, 0, 0, false);
        this.orderRepository.insertAll(orders);
        this.newOrderRepository.insertAll(newOrders);
        this.orderLineRepository.insertAll(orderLines);
        LOGGER.log(INFO, "Order finished cleanup");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // GET
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Object getAsJson(String uri) throws RuntimeException {
        String[] uriSplit = uri.split("/");
        String table = uriSplit.length > 1 ? uriSplit[1] : "";
        switch (table) {
            case "order" -> {
                int orderId = Integer.parseInt(uriSplit[uriSplit.length - 3]);
                int distId  = Integer.parseInt(uriSplit[uriSplit.length - 2]);
                int wareId  = Integer.parseInt(uriSplit[uriSplit.length - 1]);
                this.transactionManager.beginTransaction(0, 0, 0, true);
                return this.orderRepository.lookupByKey(new Order.OrderId(orderId, distId, wareId));
            }
            case "history" -> {
                int id = Integer.parseInt(uriSplit[uriSplit.length - 1]);
                this.transactionManager.beginTransaction(0, 0, 0, true);
                return this.historyRepository.lookupByKey(id);
            }
            // ── HATtrick: COUNT(*) + freshness txnnums ────────────────────
            // GET /query/CA1_ORDERS     -> {"rows": [[count, txnnum_1, txnnum_2]]}
            // GET /query/CA2_ORDER_LINE -> {"rows": [[count, txnnum_1, txnnum_2]]}
            // GET /query/CA3_HISTORY    -> {"rows": [[count, txnnum_1, txnnum_2]]}
            case "query" -> {
                String queryType = uriSplit.length > 2 ? uriSplit[2] : "";
                return executeCountQuery(queryType);
            }
            // ─────────────────────────────────────────────────────────────
            default -> {
                LOGGER.log(WARNING, "URI not recognized: " + uri);
                return "";
            }
        }
    }

    /**
     * Opens one consistent MVCC snapshot, reads COUNT(*) from the target table,
     * then reads the latest committed txnnum for each T-client from FRESHNESS.
     *
     * All reads are under the same snapshot, so count and txnnums are consistent.
     * If T-client j committed txn N before the query started but the snapshot
     * doesn't reflect it, txnnum in FRESHNESS will be less than N, revealing
     * the staleness gap used to compute f_Aq.
     *
     * Response shape: {"rows": [[count, txnnum_1, txnnum_2, ...]]}
     */
    private String executeCountQuery(String queryType) {
        try {
            // Read purely from lock-free AtomicLong counters maintained by
            // OrderService. No beginTransaction(), no VMS index machinery,
            // no blocking — safe to call at any time including after leader
            // disconnect (tau=0) or between batches.
            long count = switch (queryType) {
                case "CA1_ORDERS"     -> HATtrickCounters.ordersCount.get();
                case "CA2_ORDER_LINE" -> HATtrickCounters.orderLineCount.get();
                case "CA3_HISTORY"    -> HATtrickCounters.historyCount.get();
                default -> {
                    LOGGER.log(WARNING, "Unknown queryType: " + queryType);
                    yield -1L;
                }
            };

            // Build JSON string directly so the HTTP framework returns it as-is
            // rather than calling Map.toString() which produces non-JSON output.
            // Format: {"rows":[[count,txnnum_1,txnnum_2,...]]}
            StringBuilder sb = new StringBuilder("{\"rows\":[[").append(count);
            for (int j = 1; j <= numTClients; j++) {
                sb.append(',').append(HATtrickCounters.getFreshness(j));
            }
            sb.append("]]}");
            return sb.toString();

        } catch (Exception e) {
            LOGGER.log(ERROR, "executeCountQuery(" + queryType + ") failed: " + e.getMessage());
            return "{\"rows\":[]}";
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // PUT — data population
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public void put(String uri, String payload) {
        final String[] uriSplit = uri.split("/");
        String op = uriSplit[uriSplit.length - 1];
        if (op.contentEquals("load")) {
            this.transactionManager.rebuildIndexes();
            return;
        }

        Properties prop = ConfigUtils.loadProperties();
        int numWare = Integer.parseInt(prop.getProperty("num_ware"));
        boolean checkpointing = Boolean.parseBoolean(prop.getProperty("checkpointing"));
        this.transactionManager.reset();

        ForkJoinPool pool    = ForkJoinPool.commonPool();
        Future<?>[]  futures = new Future[numWare];

        LOGGER.log(INFO, "Populating order VMS...");
        long initTs = System.currentTimeMillis();

        if (checkpointing) {
            this.populateDisk(numWare, futures, pool);
        } else {
            this.populateInMemory(numWare, futures, pool);
        }

        // ── HATtrick: seed one FRESHNESS row per T-client ─────────────────
        this.numTClients = Integer.parseInt(prop.getProperty("num_t_clients", "2"));
        this.transactionManager.beginTransaction(0, 0, 0, false);
        for (int j = 1; j <= numTClients; j++) {
            this.freshnessRepository.insert(new Freshness(j, 0L));
        }
        LOGGER.log(INFO, "Seeded " + numTClients + " FRESHNESS rows");

        // ── HATtrick: snapshot initial row counts into lock-free counters ─
        // The counters start at 0 and are incremented live by OrderService.
        // We do NOT attempt to count loaded rows here (that would require
        // a beginTransaction which may block). The counts start from 0 and
        // grow with each live transaction — correct for freshness tracking.
        HATtrickCounters.ordersCount.set(0);
        HATtrickCounters.orderLineCount.set(0);
        HATtrickCounters.historyCount.set(0);
        LOGGER.log(INFO, "HATtrick counters initialised to 0 (live increments start now)");
        // ──────────────────────────────────────────────────────────────────

        long endTs = System.currentTimeMillis();
        LOGGER.log(INFO, "Finished populating order VMS in " + (endTs - initTs) + " ms");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Population helpers (unchanged from baseline)
    // ─────────────────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private void populateDisk(int numWare, Future<?>[] futures, ForkJoinPool pool) {

        final var orderRepo  = ((AbstractProxyRepository<Order.OrderId, Order>) orderRepository);
        final var orderIndex = orderRepo.getTable().underlyingPrimaryKeyIndex();

        final var orderLineRepo  = ((AbstractProxyRepository<OrderLine.OrderLineId, OrderLine>) orderLineRepository);
        final var orderLineIndex = orderLineRepo.getTable().underlyingPrimaryKeyIndex();

        for (int w_id = 1; w_id <= numWare; w_id++) {
            final int f_w_id = w_id;
            futures[w_id - 1] = pool.submit(() -> {
                LOGGER.log(INFO, "Started creating 30_000 order records for warehouse " + f_w_id);
                long internalInitTs = System.currentTimeMillis();
                for (int d_id = 1; d_id <= TPCcConstants.NUM_DIST_PER_WARE; d_id++) {
                    for (int o_id = 1; o_id <= TPCcConstants.NUM_CUST_PER_DIST; o_id++) {
                        int  carrier_id = o_id < 2101 ? randomNumber(1, 10) : -1;
                        int  ol_count   = randomNumber(5, 15);
                        Order order = new Order(o_id, d_id, f_w_id,
                                randomNumber(1, 3000), new Date(), carrier_id, ol_count, 1);
                        Object[] orderObj = orderRepo.extractFieldValuesFromEntityObject(order);
                        IKey orderKey = KeyUtils.buildRecordKey(
                                orderIndex.schema().getPrimaryKeyColumns(), orderObj);
                        synchronized (orderIndex) {
                            orderIndex.insert(orderKey, orderObj);
                        }
                        Date  ol_delivery_d = o_id < 2101 ? new Date() : null;
                        float ol_amount     = o_id < 2101 ? 0
                                : (float) (Math.floor((ThreadLocalRandom.current().nextDouble() * 9999.99) * 100) / 100.0);
                        for (int ol_id = 1; ol_id <= ol_count; ol_id++) {
                            OrderLine orderLine = new OrderLine(o_id, d_id, f_w_id, ol_id,
                                    randomNumber(1, TPCcConstants.NUM_ITEMS), f_w_id,
                                    ol_delivery_d, 5, ol_amount, makeAlphaString(26, 50));
                            Object[] olObj = orderLineRepo.extractFieldValuesFromEntityObject(orderLine);
                            IKey olKey = KeyUtils.buildRecordKey(
                                    orderLineIndex.schema().getPrimaryKeyColumns(), olObj);
                            synchronized (orderLineIndex) {
                                orderLineIndex.insert(olKey, olObj);
                            }
                        }
                    }
                }
                LOGGER.log(INFO, "Finished creating 30_000 order records for warehouse "
                        + f_w_id + " in " + (System.currentTimeMillis() - internalInitTs) + " ms");
            });
        }
        try {
            for (int w_id = 1; w_id <= numWare; w_id++) futures[w_id - 1].get();
            futures[0] = pool.submit(orderIndex::flush);
            futures[1] = pool.submit(orderLineIndex::flush);
            for (int i = 0; i < 2; i++) futures[i].get();
            this.transactionManager.rebuildIndexes();
        } catch (ExecutionException | InterruptedException e) {
            LOGGER.log(ERROR, "Error:\n" + e);
        }
    }

    private void populateInMemory(int numWare, Future<?>[] futures, ForkJoinPool pool) {
        for (int w_id = 1; w_id <= numWare; w_id++) {
            final int f_w_id = w_id;
            futures[w_id - 1] = pool.submit(() -> {
                LOGGER.log(DEBUG, "Started creating 30_000 order records for warehouse " + f_w_id);
                transactionManager.beginTransaction(-f_w_id, 0, -numWare, false);
                long internalInitTs = System.currentTimeMillis();
                for (int d_id = 1; d_id <= TPCcConstants.NUM_DIST_PER_WARE; d_id++) {
                    for (int o_id = 1; o_id <= TPCcConstants.NUM_CUST_PER_DIST; o_id++) {
                        int  carrier_id = o_id < 2101 ? randomNumber(1, 10) : -1;
                        int  ol_count   = randomNumber(5, 15);
                        Order order = new Order(o_id, d_id, f_w_id,
                                randomNumber(1, 3000), new Date(), carrier_id, ol_count, 1);
                        orderRepository.insert(order);
                        Date  ol_delivery_d = o_id < 2101 ? new Date() : null;
                        float ol_amount     = o_id < 2101 ? 0
                                : (float) (Math.floor((ThreadLocalRandom.current().nextDouble() * 9999.99) * 100) / 100.0);
                        for (int ol_id = 1; ol_id <= ol_count; ol_id++) {
                            OrderLine orderLine = new OrderLine(o_id, d_id, f_w_id, ol_id,
                                    randomNumber(1, TPCcConstants.NUM_ITEMS), f_w_id,
                                    ol_delivery_d, 5, ol_amount, makeAlphaString(26, 50));
                            orderLineRepository.insert(orderLine);
                        }
                    }
                }
                LOGGER.log(DEBUG, "Finished creating 30_000 order records for warehouse "
                        + f_w_id + " in " + (System.currentTimeMillis() - internalInitTs) + " ms");
            });
        }
        try {
            for (int w_id = 1; w_id <= numWare; w_id++) futures[w_id - 1].get();
        } catch (ExecutionException | InterruptedException e) {
            LOGGER.log(ERROR, "Error:\n" + e);
        }
    }
}