"use client";

import { useEffect, useState } from "react";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from "recharts";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Activity, Zap, AlertTriangle } from "lucide-react";

export default function LiveCharts() {
  const [data, setData] = useState<any[]>([]);
  const [latestMsg, setLatestMsg] = useState<any>(null);
  const [connectionStatus, setConnectionStatus] = useState("Connecting...");

  useEffect(() => {
    // Connect to the Render/Local FastAPI SSE stream
    const eventSource = new EventSource("http://localhost:8001/api/stream");

    eventSource.onopen = () => {
      setConnectionStatus("Connected (Live)");
    };

    eventSource.onmessage = (event) => {
      try {
        const parsedData = JSON.parse(event.data);
        // Format timestamp for chart X-axis
        const time = new Date(parsedData.timestamp).toLocaleTimeString();
        const chartPoint = {
          time,
          enteringTemp: parsedData.Entering_Chilled_Water_Temperature_Sensor,
          leavingTemp: parsedData.Leaving_Chilled_Water_Temperature_Sensor,
          power: parsedData.System_Power_Consumption_kW,
        };

        setLatestMsg(parsedData);

        setData((prevData) => {
          const newData = [...prevData, chartPoint];
          // Keep only the last 20 data points on the chart
          if (newData.length > 20) {
            return newData.slice(newData.length - 20);
          }
          return newData;
        });
      } catch (err) {
        console.error("Error parsing SSE data", err);
      }
    };

    eventSource.onerror = (err) => {
      console.error("SSE Error:", err);
      setConnectionStatus("Disconnected / Reconnecting...");
    };

    return () => {
      eventSource.close();
    };
  }, []);

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center mb-4">
        <h2 className="text-xl font-semibold">Real-Time Kafka Stream</h2>
        <Badge variant={connectionStatus.includes("Live") ? "default" : "destructive"} className="px-3 py-1">
          <Activity className="w-4 h-4 mr-2" />
          {connectionStatus}
        </Badge>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <Card className="bg-card">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm text-muted-foreground font-medium flex items-center">
              <Zap className="w-4 h-4 mr-2 text-yellow-500" />
              Latest Power Consumption
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-bold">
              {latestMsg ? `${latestMsg.System_Power_Consumption_kW} kW` : "---"}
            </div>
            <p className="text-xs text-muted-foreground mt-1">
              Device: {latestMsg ? latestMsg.Device_ID : "Waiting for data..."}
            </p>
          </CardContent>
        </Card>

        <Card className="bg-card">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm text-muted-foreground font-medium flex items-center">
              <Activity className="w-4 h-4 mr-2 text-blue-500" />
              Temp Delta (Entering - Leaving)
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-bold">
              {latestMsg
                ? `${(
                    latestMsg.Entering_Chilled_Water_Temperature_Sensor -
                    latestMsg.Leaving_Chilled_Water_Temperature_Sensor
                  ).toFixed(2)} °F`
                : "---"}
            </div>
          </CardContent>
        </Card>

        <Card className="bg-card">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm text-muted-foreground font-medium flex items-center">
              <AlertTriangle className="w-4 h-4 mr-2 text-red-500" />
              Latest Fault Code
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-bold">
              {latestMsg ? (
                latestMsg.Active_Fault_Code === "0" ? (
                  <span className="text-green-500">Normal</span>
                ) : (
                  <span className="text-red-500">{latestMsg.Active_Fault_Code}</span>
                )
              ) : (
                "---"
              )}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Line Chart */}
      <Card className="bg-card mt-6">
        <CardHeader>
          <CardTitle>Live Chiller Temperature Stream</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-[400px] w-full">
            {data.length > 0 ? (
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={data}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                  <XAxis dataKey="time" stroke="#888" />
                  <YAxis stroke="#888" domain={['auto', 'auto']} />
                  <Tooltip
                    contentStyle={{ backgroundColor: "#1e293b", border: "none" }}
                    labelStyle={{ color: "#94a3b8" }}
                  />
                  <Legend />
                  <Line
                    type="monotone"
                    dataKey="enteringTemp"
                    name="Entering Temp (°F)"
                    stroke="#ef4444"
                    strokeWidth={2}
                    dot={{ r: 4 }}
                    activeDot={{ r: 6 }}
                    isAnimationActive={false}
                  />
                  <Line
                    type="monotone"
                    dataKey="leavingTemp"
                    name="Leaving Temp (°F)"
                    stroke="#3b82f6"
                    strokeWidth={2}
                    dot={{ r: 4 }}
                    activeDot={{ r: 6 }}
                    isAnimationActive={false}
                  />
                </LineChart>
              </ResponsiveContainer>
            ) : (
              <div className="h-full w-full flex items-center justify-center text-muted-foreground">
                Waiting for streaming data...
              </div>
            )}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
