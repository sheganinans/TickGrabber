#!/bin/bash
run() {
  while true; do
    echo "run"
    dotnet run -c release &
    echo "sleeping killer"
    sleep 2500
    pkill -f "TickGrabber"
    echo "going again"
  done
}
run