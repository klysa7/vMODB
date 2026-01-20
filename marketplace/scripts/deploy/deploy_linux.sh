#!/bin/bash

# Access individual command-line arguments
echo "Script name: $0"
echo "Number of arguments: $#"
echo "The arguments passed: $*"

help_="--help"
param1="$1"

if [ "$param1" = "$help_" ]; then
    echo "It is expected that the script runs in the marketplace project's root folder."
    echo "You can specify the apps using the following pattern:"
    echo "<app-id1> ... <app-idn>"
    exit 1
fi

current_dir=$(pwd)
echo "Current dir is" $current_dir
echo ""

if [ $# -eq 0 ]; then
  echo "ERROR: No arguments passed"
  exit 1
fi

if test -d "$(pwd)/proxy"; then
  echo "Initializing deploy of microservices..."
else
  echo "ERROR: Run the script in the marketplace project's root folder!"
  exit 1
fi

# ---- NEW: logs folder + helper to start in background ----
log_dir="$current_dir/logs"
mkdir -p "$log_dir"

start_bg () {
  name="$1"
  jar_rel="$2"

  # If jar path doesn't exist, fail fast with a good message
  if [ ! -f "$current_dir/$jar_rel" ]; then
    echo "ERROR: Missing jar for $name at: $current_dir/$jar_rel"
    return 1
  fi

  # Better "already running" detection (matches the jar name)
  if pgrep -f "$jar_rel" >/dev/null; then
    echo "$name already running"
  else
    echo "Initializing $name..."
    java --enable-preview \
      --add-exports java.base/jdk.internal.misc=ALL-UNNAMED \
      --add-opens java.base/jdk.internal.util=ALL-UNNAMED \
      -jar "$current_dir/$jar_rel" \
      > "$log_dir/$name.log" 2>&1 &
  fi
}
# ---------------------------------------------------------
if echo "$*" | grep -q gw; then
  start_bg "Gateway" "calcite/target/calcite-1.0-SNAPSHOT.jar"
fi

if echo "$*" | grep -q cart; then
  start_bg "Cart" "cart/target/cart-1.0-SNAPSHOT-jar-with-dependencies.jar"
fi

if echo "$*" | grep -q product; then
  start_bg "Product" "product/target/product-1.0-SNAPSHOT-jar-with-dependencies.jar"
fi

if echo "$*" | grep -q stock; then
  start_bg "Stock" "stock/target/stock-1.0-SNAPSHOT-jar-with-dependencies.jar"
fi

if echo "$*" | grep -q order; then
  start_bg "Order" "order/target/order-1.0-SNAPSHOT-jar-with-dependencies.jar"
fi

if echo "$*" | grep -q payment; then
  start_bg "Payment" "payment/target/payment-1.0-SNAPSHOT-jar-with-dependencies.jar"
fi

if echo "$*" | grep -q shipment; then
  start_bg "Shipment" "shipment/target/shipment-1.0-SNAPSHOT-jar-with-dependencies.jar"
fi

if echo "$*" | grep -q seller; then
  start_bg "Seller" "seller/target/seller-1.0-SNAPSHOT-jar-with-dependencies.jar"
fi

if echo "$*" | grep -q customer; then
  start_bg "Customer" "customer/target/customer-1.0-SNAPSHOT-jar-with-dependencies.jar"
fi

if echo "$*" | grep -q proxy; then
  # IMPORTANT: proxy should start after VMSes, and usually in foreground so you see logs
  if pgrep -f "proxy/target/proxy-1.0-SNAPSHOT-jar-with-dependencies.jar" >/dev/null; then
    echo "Proxy already running"
  else
    echo "Waiting 2 sec for microservices before setting up the proxy (coordinator)..."
    sleep 2
    echo "Initializing Proxy..."
    java --enable-preview \
      --add-exports java.base/jdk.internal.misc=ALL-UNNAMED \
      --add-opens java.base/jdk.internal.util=ALL-UNNAMED \
      -jar "$current_dir/proxy/target/proxy-1.0-SNAPSHOT-jar-with-dependencies.jar" \
      2>&1 | tee "$log_dir/proxy.log"
  fi
fi

echo ""
echo "Done. Logs are in: $log_dir"
echo "Examples:"
echo "  tail -f $log_dir/cart.log"
echo "  tail -f $log_dir/proxy.log"
