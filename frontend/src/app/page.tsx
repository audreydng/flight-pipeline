// src/app/page.tsx
"use client";

import { useState, useEffect, useCallback, useMemo } from "react";
import dynamic from "next/dynamic";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  Plane, MapPin, Gauge, ArrowUp, Compass, Database,
  BarChart3, Activity, Radio, ChevronRight,
  Zap, Layers, RefreshCw, Clock,
} from "lucide-react";
import {
  getLiveFlights, getMongoStats, getPgFlights,
  getDelayDistribution, getTopAirlines, getPipelineStatus,
  type Flight, type PgFlight, type DelayItem, type AirlineItem, type PipelineStatus,
} from "@/lib/api";

// Dynamic import for map (no SSR)
const FlightMap = dynamic(() => import("@/components/dashboard/FlightMap"), {
  ssr: false,
  loading: () => (
    <div className="w-full h-[500px] bg-slate-900 rounded-xl flex items-center justify-center">
      <div className="text-slate-500 text-sm flex items-center gap-2">
        <div className="w-4 h-4 border-2 border-cyan-500 border-t-transparent rounded-full animate-spin" />
        Loading map...
      </div>
    </div>
  ),
});

function StatusDot({ ok }: { ok: boolean }) {
  return <div className={`w-2 h-2 rounded-full ${ok ? "bg-emerald-400" : "bg-red-400"}`} />;
}

export default function DashboardPage() {
  const [flights, setFlights] = useState<Flight[]>([]);
  const [pgFlights, setPgFlights] = useState<PgFlight[]>([]);
  const [delayDist, setDelayDist] = useState<DelayItem[]>([]);
  const [topAirlines, setTopAirlines] = useState<AirlineItem[]>([]);
  const [pipeline, setPipeline] = useState<PipelineStatus | null>(null);
  const [mongoCount, setMongoCount] = useState(0);
  const [pgCount, setPgCount] = useState(0);
  const [sel, setSel] = useState<number | null>(null);
  const [lastUpdate, setLastUpdate] = useState<string>("");
  const [loading, setLoading] = useState(true);

  // Fetch all data
  const fetchData = useCallback(async () => {
    const [liveRes, mongoRes, pgRes, delayRes, airRes, statusRes] =
      await Promise.all([
        getLiveFlights(),
        getMongoStats(),
        getPgFlights(),
        getDelayDistribution(),
        getTopAirlines(),
        getPipelineStatus(),
      ]);

    if (liveRes) {
      setFlights([...liveRes.active_flights, ...liveRes.landed_flights]);
    }
    if (mongoRes) setMongoCount(mongoRes.total_records);
    if (pgRes) {
      setPgFlights(pgRes.flights);
      setPgCount(pgRes.count);
    }
    if (delayRes) setDelayDist(delayRes.distribution);
    if (airRes) setTopAirlines(airRes.airlines);
    if (statusRes) setPipeline(statusRes.pipeline);

    setLastUpdate(new Date().toLocaleTimeString());
    setLoading(false);
  }, []);

  // Auto refresh every 3 seconds
  useEffect(() => {
    const timeout = setTimeout(() => {
      void fetchData();
    }, 0);
    const iv = setInterval(() => {
      void fetchData();
    }, 3000);
    return () => {
      clearTimeout(timeout);
      clearInterval(iv);
    };
  }, [fetchData]);

  const activeFlights = useMemo(() => flights.filter((f) => f.flight_status === "active"), [flights]);
  const selFlight = useMemo(() => flights.find((f) => f.flight_id === sel), [flights, sel]);

  const totalDelay = useMemo(() => delayDist.reduce((s, d) => s + d.count, 0), [delayDist]);
  const maxAirline = topAirlines.length > 0 ? topAirlines[0].flight_count || 1 : 1;

  const handleSelect = useCallback((id: number | null) => {
    setSel((prev) => (prev === id ? null : id));
  }, []);

  return (
    <div className="min-h-screen bg-slate-950 text-white">
      {/* HEADER */}
      <header className="sticky top-0 z-50 border-b border-white/10 bg-slate-950/90 backdrop-blur-xl">
        <div className="max-w-7xl mx-auto px-4 py-3 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="p-2 rounded-xl bg-gradient-to-br from-cyan-500/20 to-blue-500/20 border border-cyan-500/20">
              <Plane className="w-5 h-5 text-cyan-400" />
            </div>
            <div>
              <h1 className="text-lg font-bold tracking-tight">Flight Data Pipeline</h1>
              <p className="text-xs text-slate-500">
                Live data from Kafka · Spark · MongoDB · Airflow · PostgreSQL
              </p>
            </div>
          </div>
          <div className="flex items-center gap-4">
            <div className="flex items-center gap-3 text-xs text-slate-400">
              <span className="flex items-center gap-1.5">
                <StatusDot ok={pipeline?.mongodb?.status === "connected"} />
                MongoDB
              </span>
              <span className="flex items-center gap-1.5">
                <StatusDot ok={pipeline?.postgresql?.status === "connected"} />
                PostgreSQL
              </span>
            </div>
            <div className="text-xs text-slate-500">Updated: {lastUpdate}</div>
            <Button variant="outline" size="sm" onClick={fetchData} className="gap-1.5">
              <RefreshCw className="w-3.5 h-3.5" />Refresh
            </Button>
          </div>
        </div>
      </header>

      {/* PIPELINE STATUS BAR */}
      <div className="border-b border-white/5 bg-slate-900/50">
        <div className="max-w-7xl mx-auto px-4 py-2.5 overflow-x-auto">
          <div className="flex items-center justify-center gap-1 min-w-max">
            {[
              { l: "API", Icon: Radio, c: "text-cyan-400", v: activeFlights.length },
              { l: "Kafka", Icon: Zap, c: "text-orange-400", v: "Live" },
              { l: "Spark", Icon: Activity, c: "text-yellow-400", v: "Stream" },
              { l: "MongoDB", Icon: Database, c: "text-green-400", v: mongoCount },
              { l: "Airflow", Icon: Layers, c: "text-blue-400", v: "DAG" },
              { l: "PostgreSQL", Icon: Database, c: "text-indigo-400", v: pgCount },
              { l: "Reports", Icon: BarChart3, c: "text-purple-400", v: "CSV" },
            ].map((n, i) => (
              <div key={i} className="flex items-center gap-1">
                <div className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg bg-white/5 border border-white/10">
                  <n.Icon className={`w-4 h-4 ${n.c}`} />
                  <span className="text-xs font-medium text-slate-300">{n.l}</span>
                  <Badge variant="secondary" className="text-xs px-1.5 py-0 h-5">
                    {n.v}
                  </Badge>
                </div>
                {i < 6 && <ChevronRight className="w-3 h-3 text-slate-700" />}
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* MAIN */}
      <main className="max-w-7xl mx-auto p-4">
        {loading ? (
          <div className="flex items-center justify-center h-96">
            <div className="text-slate-500 flex items-center gap-3">
              <div className="w-6 h-6 border-2 border-cyan-500 border-t-transparent rounded-full animate-spin" />
              Connecting to pipeline...
            </div>
          </div>
        ) : (
          <Tabs defaultValue="map" className="space-y-4">
            <TabsList className="bg-slate-900/80 border border-white/5">
              <TabsTrigger value="map" className="gap-1.5 data-[state=active]:bg-white/10">
                <MapPin className="w-3.5 h-3.5" />Live Map
              </TabsTrigger>
              <TabsTrigger value="flights" className="gap-1.5 data-[state=active]:bg-white/10">
                <Plane className="w-3.5 h-3.5" />Flights
              </TabsTrigger>
              <TabsTrigger value="reports" className="gap-1.5 data-[state=active]:bg-white/10">
                <BarChart3 className="w-3.5 h-3.5" />Reports
              </TabsTrigger>
            </TabsList>

            {/* MAP TAB */}
            <TabsContent value="map" className="mt-0">
              <div className="flex gap-4 flex-col lg:flex-row">
                <Card className="flex-1 bg-slate-900/60 border-white/5 overflow-hidden">
                  <FlightMap flights={flights} selected={sel} onSelect={handleSelect} />
                </Card>

                <div className="w-full lg:w-80 space-y-4">
                  {selFlight ? (
                    <Card className="bg-slate-900/60 border-white/5">
                      <CardHeader className="pb-2">
                        <div className="flex items-center justify-between">
                          <CardTitle className="text-sm flex items-center gap-2">
                            <Plane className="w-4 h-4 text-cyan-400" />
                            Flight #{selFlight.flight_id}
                          </CardTitle>
                          <Badge variant={selFlight.flight_status === "active" ? "default" : "secondary"}>
                            {selFlight.flight_status}
                          </Badge>
                        </div>
                        <p className="text-xs text-slate-400">{selFlight.airline}</p>
                      </CardHeader>
                      <CardContent className="space-y-4">
                        <div className="bg-slate-800/60 rounded-xl p-3">
                          <div className="flex items-center justify-between text-sm font-semibold">
                            <div className="text-center">
                              <div className="text-cyan-400 text-lg">{selFlight.departure_city}</div>
                              <div className="text-xs text-slate-500">{selFlight.departure_airport}</div>
                            </div>
                            <Plane className="w-4 h-4 text-yellow-400 mx-2" />
                            <div className="text-center">
                              <div className="text-pink-400 text-lg">{selFlight.arrival_city}</div>
                              <div className="text-xs text-slate-500">{selFlight.arrival_airport}</div>
                            </div>
                          </div>
                        </div>
                        <div className="grid grid-cols-2 gap-2">
                          {[
                            { Icon: ArrowUp, c: "text-cyan-400", l: "Altitude", v: (selFlight.current_altitude_m || 0).toLocaleString() + "m" },
                            { Icon: Gauge, c: "text-yellow-400", l: "Speed", v: (selFlight.current_speed_km_h || selFlight.speed || 0) + " km/h" },
                            { Icon: Compass, c: "text-emerald-400", l: "Heading", v: Math.round(selFlight.direction || 0) + "°" },
                            { Icon: MapPin, c: "text-pink-400", l: "Distance", v: (selFlight.distance_travelled_km || 0) + " km" },
                          ].map((s, i) => (
                            <div key={i} className="bg-slate-800/40 rounded-lg p-2">
                              <div className="flex items-center gap-1.5 text-xs text-slate-500 mb-0.5">
                                <s.Icon className={`w-3.5 h-3.5 ${s.c}`} />{s.l}
                              </div>
                              <div className="text-sm font-mono font-semibold text-slate-200">{s.v}</div>
                            </div>
                          ))}
                        </div>
                      </CardContent>
                    </Card>
                  ) : (
                    <Card className="bg-slate-900/60 border-white/5">
                      <CardContent className="py-12 text-center text-slate-500 text-sm">
                        <Plane className="w-8 h-8 mx-auto mb-3 opacity-30" />
                        Click a plane on the map
                      </CardContent>
                    </Card>
                  )}

                  <Card className="bg-slate-900/60 border-white/5">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-xs text-slate-400">
                        Active Flights ({activeFlights.length})
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="p-0">
                      <ScrollArea className="h-56">
                        <div className="px-4 pb-3 space-y-1">
                          {activeFlights.slice(0, 15).map((f) => (
                            <div
                              key={f.flight_id}
                              onClick={() => setSel(f.flight_id)}
                              className={`flex items-center justify-between px-2.5 py-2 rounded-lg text-xs cursor-pointer transition-all border ${
                                sel === f.flight_id
                                  ? "bg-cyan-500/10 border-cyan-500/20"
                                  : "hover:bg-white/5 border-transparent"
                              }`}
                            >
                              <div className="flex items-center gap-2">
                                <span className="text-yellow-400 font-mono font-semibold">
                                  #{f.flight_id}
                                </span>
                                <span className="text-slate-500 truncate max-w-32">
                                  {f.departure_city} → {f.arrival_city}
                                </span>
                              </div>
                              <span className="text-slate-500">{f.airline?.slice(0, 10)}</span>
                            </div>
                          ))}
                          {activeFlights.length === 0 && (
                            <p className="text-slate-600 text-center py-4">No active flights</p>
                          )}
                        </div>
                      </ScrollArea>
                    </CardContent>
                  </Card>
                </div>
              </div>
            </TabsContent>

            {/* FLIGHTS TAB */}
            <TabsContent value="flights">
              <Card className="bg-slate-900/60 border-white/5">
                <CardHeader>
                  <CardTitle className="text-sm flex items-center gap-2">
                    <Database className="w-4 h-4 text-indigo-400" />
                    PostgreSQL — Landed Flights ({pgCount})
                  </CardTitle>
                </CardHeader>
                <CardContent className="p-0 overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="text-xs text-slate-500 uppercase border-b border-white/5">
                        <th className="text-left py-3 px-4">ID</th>
                        <th className="text-left py-3 px-4">Airline</th>
                        <th className="text-left py-3 px-4">Route</th>
                        <th className="text-left py-3 px-4">Status</th>
                        <th className="text-left py-3 px-4">Delay</th>
                      </tr>
                    </thead>
                    <tbody>
                      {pgFlights.slice(0, 30).map((f) => (
                        <tr key={f.id} className="border-b border-white/5 hover:bg-white/5">
                          <td className="py-2.5 px-4 font-mono text-cyan-400">#{f.flight_id}</td>
                          <td className="py-2.5 px-4 text-slate-300">{f.airline}</td>
                          <td className="py-2.5 px-4 text-slate-400">
                            {f.departure_city} → {f.arrival_city}
                          </td>
                          <td className="py-2.5 px-4">
                            <Badge variant="secondary" className="text-xs">
                              {f.flight_status}
                            </Badge>
                          </td>
                          <td className="py-2.5 px-4">
                            <Badge
                              className={`text-xs ${
                                f.delayed_status === "on_time"
                                  ? "bg-emerald-500/20 text-emerald-400"
                                  : f.delayed_status === "slightly_delayed"
                                  ? "bg-amber-500/20 text-amber-400"
                                  : "bg-red-500/20 text-red-400"
                              }`}
                            >
                              {f.delayed_status || "—"}
                            </Badge>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {pgFlights.length === 0 && (
                    <p className="text-slate-500 text-sm text-center py-8">
                      No data yet. Run Airflow DAG to process landed flights.
                    </p>
                  )}
                </CardContent>
              </Card>
            </TabsContent>

            {/* REPORTS TAB */}
            <TabsContent value="reports">
              <div className="grid md:grid-cols-2 gap-4">
                <Card className="bg-slate-900/60 border-white/5">
                  <CardHeader>
                    <CardTitle className="text-sm flex items-center gap-2">
                      <Clock className="w-4 h-4 text-cyan-400" />Delay Distribution
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    {delayDist.length > 0 ? (
                      delayDist.map((d) => {
                        const colors: Record<string, { bar: string; text: string }> = {
                          on_time: { bar: "bg-emerald-500", text: "text-emerald-400" },
                          slightly_delayed: { bar: "bg-amber-500", text: "text-amber-400" },
                          too_late: { bar: "bg-red-500", text: "text-red-400" },
                        };
                        const c = colors[d.delayed_status] || colors.on_time;
                        return (
                          <div key={d.delayed_status}>
                            <div className="flex justify-between text-xs mb-1.5">
                              <span className={`font-medium ${c.text}`}>
                                {d.delayed_status?.replace("_", " ")}
                              </span>
                              <span className="text-slate-400 font-mono">
                                {d.count} ({totalDelay ? Math.round((d.count / totalDelay) * 100) : 0}%)
                              </span>
                            </div>
                            <div className="h-3 bg-slate-800 rounded-full overflow-hidden">
                              <div
                                className={`h-full rounded-full transition-all duration-700 ${c.bar}`}
                                style={{ width: `${totalDelay ? (d.count / totalDelay) * 100 : 0}%` }}
                              />
                            </div>
                          </div>
                        );
                      })
                    ) : (
                      <p className="text-slate-500 text-sm text-center py-4">
                        Run Airflow DAG to see delay data
                      </p>
                    )}
                  </CardContent>
                </Card>

                <Card className="bg-slate-900/60 border-white/5">
                  <CardHeader>
                    <CardTitle className="text-sm flex items-center gap-2">
                      <BarChart3 className="w-4 h-4 text-purple-400" />Top Airlines
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-2.5">
                    {topAirlines.length > 0 ? (
                      topAirlines.map((a, i) => (
                        <div key={a.airline} className="flex items-center gap-2">
                          <span className="text-xs font-bold text-cyan-400 w-5">#{i + 1}</span>
                          <span className="text-xs text-slate-300 w-32 truncate">{a.airline}</span>
                          <div className="flex-1 h-2.5 bg-slate-800 rounded-full overflow-hidden">
                            <div
                              className="h-full bg-gradient-to-r from-cyan-500 to-purple-500 rounded-full"
                              style={{
                                width: `${((a.flight_count || 0) / maxAirline) * 100}%`,
                              }}
                            />
                          </div>
                          <span className="text-xs font-mono text-slate-400 w-6 text-right">
                            {a.flight_count}
                          </span>
                        </div>
                      ))
                    ) : (
                      <p className="text-slate-500 text-sm text-center py-4">No data yet</p>
                    )}
                  </CardContent>
                </Card>
              </div>
            </TabsContent>
          </Tabs>
        )}
      </main>
    </div>
  );
}
