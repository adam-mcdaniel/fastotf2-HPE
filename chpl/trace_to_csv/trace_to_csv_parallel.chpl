// Copyright Hewlett Packard Enterprise Development LP.
// Untested, experimental Chapel code for reading OTF2 traces in parallel
// Has compilation errors currently

module TraceToCSVParallel {
  use OTF2;
  use Time;
  use List;
  use Map;
  use CallGraphModule;
  use IO;
  import Math.inf;

  // This record should be in a Chapel OTF2 module since it is common for all readers
  // but for simplicity, we keep it here for now.
  record ClockProperties {
    // See https://perftools.pages.jsc.fz-juelich.de/cicd/otf2/tags/latest/html/group__records__definition.html#ClockProperties
    var timerResolution: uint(64);
    var globalOffset: uint(64);
    var traceLength: uint(64);
    var realtimeTimestamp: uint(64);
  }

  proc registerClockProperties(userData: c_ptr(void),
                              timerResolution: uint(64),
                              globalOffset: uint(64),
                              traceLength: uint(64),
                              realtimeTimestamp: uint(64)): OTF2_CallbackCode {
    var defContextPtr = userData: c_ptr(DefCallbackContext);
    if defContextPtr == nil then return OTF2_CALLBACK_ERROR;
    ref defContext = defContextPtr.deref();
    ref clockProps = defContext.clockProps;
    clockProps.timerResolution = timerResolution;
    clockProps.globalOffset = globalOffset;
    clockProps.traceLength = traceLength;
    clockProps.realtimeTimestamp = realtimeTimestamp;
    writeln("Trace Clock Properties:");
    writeln(" Timer Resolution. : ", clockProps.timerResolution);
    writeln(" Global Offset     : ", clockProps.globalOffset);
    writeln(" Trace Length      : ", clockProps.traceLength);
    writeln(" Realtime Timestamp: ", clockProps.realtimeTimestamp);
    return OTF2_CALLBACK_SUCCESS;
  }

  // These records should be classes and moved into a proper Chapel OTF2 module
  // but for simplicity, we keep them here for now.
  // They are also not feature complete but sufficient for the current needs.
  record LocationGroup {
    var name: string;
    var creatingLocationGroup: string;
  }
  record Location {
    var name: string;
    var group: OTF2_LocationGroupRef;
  }

  record MetricMember {
    var name: string;
    var unit: string;
  }

  // Metric class and instance should inherit from a common Metric base class
  record MetricClass {
    var numberOfMetrics: c_uint8;
    var firstMemberID: OTF2_MetricMemberRef;  // Store just the first member ID directly
  }

  record MetricInstance {
    var metricClass: OTF2_MetricRef;
    var recorder: OTF2_LocationRef;
  }

  record MetricDefContext {
    var metricClassIds: domain(OTF2_MetricRef);
    var metricClassTable: [metricClassIds] MetricClass;
    var metricInstanceIds: domain(OTF2_MetricRef);
    var metricInstanceTable: [metricInstanceIds] MetricInstance;
    var metricMemberIds: domain(OTF2_MetricMemberRef);
    var metricMemberTable: [metricMemberIds] MetricMember;
    var metricClassRecorderIds: domain(OTF2_MetricRef);
    var metricClassRecorderTable: [metricClassRecorderIds] OTF2_LocationRef;
  }

  record DefCallbackContext {
    var locationGroupIds: domain(OTF2_LocationGroupRef);
    var locationGroupTable: [locationGroupIds] LocationGroup;
    var locationIds: domain(OTF2_LocationRef);
    var locationTable: [locationIds] Location;
    var regionIds: domain(OTF2_RegionRef);
    var regionTable: [regionIds] string;
    var stringIds: domain(OTF2_StringRef);
    var stringTable: [stringIds] string;
    var clockProps: ClockProperties;
    var metricDefContext: MetricDefContext;
  }

  // --- Definition callbacks ---
  proc GlobDefString_Register(userData: c_ptr(void),
                              strRef: OTF2_StringRef,
                              strName: c_ptrConst(c_uchar)):
                              OTF2_CallbackCode {
    var ctxPtr = userData: c_ptr(DefCallbackContext);
    if ctxPtr == nil then return OTF2_CALLBACK_ERROR;
    ref ctx = ctxPtr.deref();
    // Add string to the lookup table
    ctx.stringIds += strRef;
    if strName != nil {
      try! ctx.stringTable[strRef] = string.createCopyingBuffer(strName);
    } else {
      ctx.stringTable[strRef] = "UnknownString";
    }
    // writeln("Registered string: ", ctx.stringTable[str]);
    return OTF2_CALLBACK_SUCCESS;
  }

  proc GlobDefLocationGroup_Register(userData: c_ptr(void),
                                     self : OTF2_LocationGroupRef,
                                     name : OTF2_StringRef,
                                     locationGroupType : OTF2_LocationGroupType,
                                     systemTreeParent : OTF2_SystemTreeNodeRef,
                                     creatingLocationGroup : OTF2_LocationGroupRef): OTF2_CallbackCode {
    // Get the reference to the context record
    var ctxPtr = userData: c_ptr(DefCallbackContext);
    if ctxPtr == nil then return OTF2_CALLBACK_ERROR;
    ref ctx = ctxPtr.deref();
    // Lookup name in string table
    const groupName = if ctx.stringIds.contains(name) && ctx.stringTable[name] != "" then ctx.stringTable[name] else "UnknownGroup";
    // Check if this location has a creating group
    // if creatingLocationGroup != 0 then
    //   writeln("Location group ", groupName, " created by group ID ", creatingLocationGroup);
    // else
    //   writeln("Location group ", groupName, " has no creating group");
    const creatingGroupName = if ctx.locationGroupIds.contains(creatingLocationGroup) then ctx.locationGroupTable[creatingLocationGroup].name else "None";
    // Add location group to the lookup table
    ctx.locationGroupIds += self;
    ctx.locationGroupTable[self] = new LocationGroup(name=groupName, creatingLocationGroup=creatingGroupName);
    // writeln("Registered location group: ", ctx.locationGroupTable[self]);
    return OTF2_CALLBACK_SUCCESS;
  }

  proc GlobDefLocation_Register(userData: c_ptr(void),
                                location: OTF2_LocationRef,
                                name: OTF2_StringRef,
                                locationType: OTF2_LocationType,
                                numberOfEvents: c_uint64,
                                locationGroup: OTF2_LocationGroupRef):
                                OTF2_CallbackCode {
    // Get the reference to the context record
    var ctxPtr = userData: c_ptr(DefCallbackContext);
    if ctxPtr == nil then return OTF2_CALLBACK_ERROR;
    ref ctx = ctxPtr.deref();
    // Lookup name in string table
    const locName = if ctx.stringIds.contains(name) && ctx.stringTable[name] != "" then ctx.stringTable[name] else "UnknownLocation";
    ctx.locationIds += location;
    var loc = new Location(name=locName, group=locationGroup);
    ctx.locationTable[location] = loc;
    writeln("Registered location ID=", location, ": ", ctx.locationTable[location], " in group ID ", locationGroup, " (", if ctx.locationGroupIds.contains(locationGroup) then ctx.locationGroupTable[locationGroup].name else "UnknownGroup", ")");
    return OTF2_CALLBACK_SUCCESS;
  }

  proc GlobDefRegion_Register(userData: c_ptr(void),
                              region: OTF2_RegionRef,
                              name: OTF2_StringRef,
                              canonicalName: OTF2_StringRef,
                              description: OTF2_StringRef,
                              regionRole: OTF2_RegionRole,
                              paradigm: OTF2_Paradigm,
                              regionFlags: OTF2_RegionFlag,
                              sourceFile: OTF2_StringRef,
                              beginLineNumber: c_uint32,
                              endLineNumber: c_uint32):
                              OTF2_CallbackCode {
    // Get the reference to the context record
    var ctxPtr = userData: c_ptr(DefCallbackContext);
    if ctxPtr == nil then return OTF2_CALLBACK_ERROR;
    ref ctx = ctxPtr.deref();
    // Lookup name in string table
    const regionName = if ctx.stringIds.contains(name) && ctx.stringTable[name] != "" then ctx.stringTable[name] else "UnknownRegion";
    // Add region to the lookup table
    ctx.regionIds += region;
    ctx.regionTable[region] = regionName;
    // writeln("Registered region: ", regionName);
    return OTF2_CALLBACK_SUCCESS;
  }

  proc GlobDefMetricMember_Register(userData: c_ptr(void),
                                    self: OTF2_MetricMemberRef,
                                    name: OTF2_StringRef,
                                    description: OTF2_StringRef,
                                    metricType: OTF2_MetricType,
                                    mode: OTF2_MetricMode,
                                    valueType: OTF2_Type,
                                    base: OTF2_Base,
                                    exponent: c_int64,
                                    unit: OTF2_StringRef): OTF2_CallbackCode {
    var ctxPtr = userData: c_ptr(DefCallbackContext);
    if ctxPtr == nil then return OTF2_CALLBACK_ERROR;
    ref ctx = ctxPtr.deref();
    ref mctx = ctx.metricDefContext;
    mctx.metricMemberIds += self;
    const memberName = if ctx.stringIds.contains(name) && ctx.stringTable[name] != "" then ctx.stringTable[name] else "UnknownMetricMember";
    const unitName = if ctx.stringIds.contains(unit) && ctx.stringTable[unit] != "" then ctx.stringTable[unit] else "UnknownUnit";
    mctx.metricMemberTable[self] = new MetricMember(name=memberName, unit=unitName);
    // writeln("Registered metric member: ", mctx.metricMemberTable[self]);
    return OTF2_CALLBACK_SUCCESS;
  }

  proc GlobDefMetricClass_Register(userData: c_ptr(void),
                                   self: OTF2_MetricRef,
                                   numberOfMetrics: c_uint8,
                                   metricMembers: c_ptrConst(OTF2_MetricMemberRef),
                                   metricOccurrence: OTF2_MetricOccurrence,
                                   recorderKind: OTF2_RecorderKind): OTF2_CallbackCode {
    var ctxPtr = userData: c_ptr(DefCallbackContext);
    if ctxPtr == nil then return OTF2_CALLBACK_ERROR;
    ref ctx = ctxPtr.deref();
    ref mctx = ctx.metricDefContext;
    mctx.metricClassIds += self;
    const firstMember = if numberOfMetrics > 0 then metricMembers[0] else 0;
    mctx.metricClassTable[self] = new MetricClass(numberOfMetrics=numberOfMetrics, firstMemberID=firstMember);
    return OTF2_CALLBACK_SUCCESS;
  }

  proc GlobDefMetricInstance_Register(userData: c_ptr(void),
                                      self: OTF2_MetricRef,
                                      metricClass: OTF2_MetricRef,
                                      recorder: OTF2_LocationRef,
                                      metricScope: OTF2_MetricScope,
                                      scope: c_uint64): OTF2_CallbackCode {
    var ctxPtr = userData: c_ptr(DefCallbackContext);
    if ctxPtr == nil then return OTF2_CALLBACK_ERROR;
    ref ctx = ctxPtr.deref();
    ref mctx = ctx.metricDefContext;
    mctx.metricInstanceIds += self;
    mctx.metricInstanceTable[self] = new MetricInstance(metricClass=metricClass, recorder=recorder);
    // writeln("Registered metric instance ID=", self, " with class=", metricClass, " recorder=", recorder);
    return OTF2_CALLBACK_SUCCESS;
  }

  proc GlobDefMetricClassRecorder_Register(userData: c_ptr(void),
                                           metric: OTF2_MetricRef,
                                           recorder: OTF2_LocationRef): OTF2_CallbackCode {
    var ctxPtr = userData: c_ptr(DefCallbackContext);
    if ctxPtr == nil then return OTF2_CALLBACK_ERROR;
    ref ctx = ctxPtr.deref();
    ref mctx = ctx.metricDefContext;
    mctx.metricClassRecorderIds += metric;
    mctx.metricClassRecorderTable[metric] = recorder;
    // writeln("Registered metric class recorder: metric=", metric, " recorder=", recorder);
    return OTF2_CALLBACK_SUCCESS;
  }

  record EvtCallbackArgs {
    const processesToTrack: domain(string);
    const metricsToTrack: domain(string);
    const crayTimeOffset: real(64);
  }

  record EvtCallbackContext {
    const evtArgs: EvtCallbackArgs;
    var defContext: DefCallbackContext;
    var seenGroups: map(string, domain(string));
    // Call Graphs are per location group and per location (thread)
    var callGraphs: map(string, map(string, shared CallGraph));
    // Metrics recorded per location group and per location (thread)
    var metrics: map(string, map(string, list((real(64), OTF2_Type, OTF2_MetricValue))));

    proc init(evtArgs: EvtCallbackArgs,
              defContext: DefCallbackContext) {
      this.evtArgs = evtArgs;
      this.defContext = defContext;
      this.seenGroups = new map(string, domain(string));
      this.callGraphs = new map(string, map(string, shared CallGraph));
      this.metrics = new map(string, map(string, list((real(64), OTF2_Type, OTF2_MetricValue))));
    }
  }

  proc timestampToSeconds(ts: OTF2_TimeStamp, clockProps: ClockProperties): real(64) {
    if clockProps.timerResolution == 0 then
      return 0.0;
    // We use this start_time to normalize timestamps to start from zero
    // We don't use a ProgramBegin event because each MPI rank will have it's own
    // and we want a global start time
    const start_time = clockProps.globalOffset;
    return (ts - start_time):real(64) / clockProps.timerResolution;
  }

  proc getLocationAndRegionInfo(defCtx: DefCallbackContext,
                       location: OTF2_LocationRef,
                       region: OTF2_RegionRef) : (string, string, string) {
    const locName = if defCtx.locationIds.contains(location) then defCtx.locationTable[location].name else "UnknownLocation";
    var locGroup = "UnknownLocationGroup";
    if defCtx.locationIds.contains(location) {
      const groupRef = defCtx.locationTable[location].group;
      if defCtx.locationGroupIds.contains(groupRef) {
        locGroup = defCtx.locationGroupTable[groupRef].name;
      } else {
        // Try to get creating location group if exists
        const creatingGroupRef = defCtx.locationTable[location].group;
        if defCtx.locationGroupIds.contains(creatingGroupRef) then
          locGroup = defCtx.locationGroupTable[creatingGroupRef].name;
      }
    }
    const regionName = if defCtx.regionIds.contains(region) then defCtx.regionTable[region] else "UnknownRegion";
    return (locName, locGroup, regionName);
  }

  proc updateMaps(ctx: EvtCallbackContext, locGroup: string, location: string) {
    // Update seen groups
    try! {
    ref seenGroups = ctx.seenGroups;
    if !seenGroups.contains(locGroup) {
      seenGroups[locGroup] = {location};
      writeln("New group and thread: ", location, " in group ", locGroup);
    } else if !seenGroups[locGroup].contains(location) {
      seenGroups[locGroup] += location;
      writeln("New thread: ", location, " in existing group ", locGroup);
    }
    }

    try! {
    // Update call graphs
    ref callGraphs = ctx.callGraphs;
    if !callGraphs.contains(locGroup) {
      var newMap = new map(string, shared CallGraph);
      callGraphs[locGroup] = newMap;
    }
    if !callGraphs[locGroup].contains(location) {
      // writeln("Creating call graph for location: ", location, " in group ", locGroup);
      var newCallGraph = new shared CallGraph();
      try! {
        callGraphs[locGroup][location] = newCallGraph;
      }
    }
    }

    // Update metrics
    ref metrics = ctx.metrics;
    if !metrics.contains(locGroup) {
      var newMetricMap = new map(string, list((real(64), OTF2_Type, OTF2_MetricValue)));
      try! {
        metrics[locGroup] = newMetricMap;
      }
    }
  }

  proc checkEnterLeaveSkipConditions(ctx: EvtCallbackContext,
                                     locGroup: string,
                                     regionName: string): bool {
    // Check if we are tracking this process
    // I don't know how to get "all processes" in Chapel so we don't do this check for now
    // if !ctx.evtArgs.processesToTrack.contains(locGroup) then
    //   return true; // Skip this event

    // Check for other skip conditions
    const regionNameLower = regionName.toLower();
    if regionNameLower.size >= 3 {
      const prefix = regionNameLower[0..2];
      if prefix == "mpi" || prefix == "omp" || prefix == "!$o" then
        return true; // Skip this event
    }
    return false; // Do not skip
  }

  // --- Event callbacks (now operate on EvtCallbackContext) ---
  proc Enter_callback(location: OTF2_LocationRef,
                      time: OTF2_TimeStamp,
                      userData: c_ptr(void),
                      attributes: c_ptr(OTF2_AttributeList),
                      region: OTF2_RegionRef): OTF2_CallbackCode {
    //writeln("Debug: Entering Enter_store_and_count with location=", location, ", region=", region);
    // Get pointers to the context and event data
    var ctxPtr = userData: c_ptr(EvtCallbackContext);
    if ctxPtr == nil then return OTF2_CALLBACK_ERROR;
    ref ctx = ctxPtr.deref();
    ref defCtx = ctx.defContext;

    const (locName, locGroup, regionName) = getLocationAndRegionInfo(defCtx, location, region);

    if checkEnterLeaveSkipConditions(ctx, locGroup, regionName) then
      return OTF2_CALLBACK_SUCCESS;

    updateMaps(ctx, locGroup, locName);

    const timeInSeconds = timestampToSeconds(time, defCtx.clockProps);
    try! {
      ref callGraph = ctx.callGraphs[locGroup][locName];
      callGraph.enter(timeInSeconds, regionName);
    }
    return OTF2_CALLBACK_SUCCESS;
  }

  proc Leave_callback(location: OTF2_LocationRef,
                      time: OTF2_TimeStamp,
                      userData: c_ptr(void),
                      attributes: c_ptr(OTF2_AttributeList),
                      region: OTF2_RegionRef): OTF2_CallbackCode {
    //writeln("Debug: Entering Leave_store_and_count with location=", location, ", region=", region);
    // Get pointers to the context and event data
    var ctxPtr = userData: c_ptr(EvtCallbackContext);
    if ctxPtr == nil then return OTF2_CALLBACK_ERROR;
    ref ctx = ctxPtr.deref();
    ref defCtx = ctx.defContext;

    const (locName, locGroup, regionName) = getLocationAndRegionInfo(defCtx, location, region);

    if checkEnterLeaveSkipConditions(ctx, locGroup, regionName) then
      return OTF2_CALLBACK_SUCCESS;

    const timeInSeconds = timestampToSeconds(time, defCtx.clockProps);
    try! {
      ref callGraph = ctx.callGraphs[locGroup][locName];
      callGraph.leave(timeInSeconds);
    }
    return OTF2_CALLBACK_SUCCESS;
  }

  proc getMetricInfo(defCtx: DefCallbackContext,
                     metric: OTF2_MetricRef): (bool, OTF2_LocationRef, string) {
    var isMetricInstance = false;
    var recorder: OTF2_LocationRef = 0;
    var metricName: string = "";

    ref mctx = defCtx.metricDefContext;

    // Check if it's a metric instance
    if mctx.metricInstanceIds.contains(metric) {
      isMetricInstance = true;
      const instance = mctx.metricInstanceTable[metric];
      recorder = instance.recorder;
      const classRef = instance.metricClass;

      // Get metric member name
      if mctx.metricClassIds.contains(classRef) {
        const metricClass = mctx.metricClassTable[classRef];
        const firstMemberRef = metricClass.firstMemberID;
        if mctx.metricMemberIds.contains(firstMemberRef) {
          const member = mctx.metricMemberTable[firstMemberRef];
          metricName = member.name;
        } else {
          metricName = "UnknownMetricMember_" + firstMemberRef:string;
        }
      } else {
        metricName = "UnknownMetricClass_" + classRef:string;
      }
    } else if mctx.metricClassRecorderIds.contains(metric) {
      // It's a metric class with recorder
      isMetricInstance = false;
      recorder = mctx.metricClassRecorderTable[metric];

      if mctx.metricClassIds.contains(metric) {
        const metricClass = mctx.metricClassTable[metric];
        const firstMemberRef = metricClass.firstMemberID;
        if mctx.metricMemberIds.contains(firstMemberRef) {
          const member = mctx.metricMemberTable[firstMemberRef];
          metricName = member.name;
        } else {
          metricName = "UnknownMetricMember_" + firstMemberRef:string;
        }
      } else {
        metricName = "UnknownMetricClass_" + metric:string;
      }
    } else {
      metricName = "UnknownMetric_" + metric:string;
    }

    return (isMetricInstance, recorder, metricName);
  }

  proc Metric_callback(location: OTF2_LocationRef,
                       time: OTF2_TimeStamp,
                       userData: c_ptr(void),
                       attributes: c_ptr(OTF2_AttributeList),
                       metric: OTF2_MetricRef,
                       numberOfMetrics: c_uint8,
                       typeIDs: c_ptrConst(OTF2_Type),
                       metricValues: c_ptrConst(OTF2_MetricValue)): OTF2_CallbackCode {
    var ctxPtr = userData: c_ptr(EvtCallbackContext);
    if ctxPtr == nil then return OTF2_CALLBACK_ERROR;
    ref ctx = ctxPtr.deref();
    ref defCtx = ctx.defContext;

    const (isMetricInstance, recorder, metricName) = getMetricInfo(defCtx, metric);

    // Check if we are tracking this metric
    if !ctx.evtArgs.metricsToTrack.isEmpty() && !ctx.evtArgs.metricsToTrack.contains(metricName) then
      return OTF2_CALLBACK_SUCCESS;

    // For metric instances, the location parameter is the recorder
    // For metric classes, we use the recorder from the metric class recorder table
    const actualRecorder = if isMetricInstance then location else recorder;

    // Get location group info
    var locGroup = "UnknownLocationGroup";
    if defCtx.locationIds.contains(actualRecorder) {
      const groupRef = defCtx.locationTable[actualRecorder].group;
      if defCtx.locationGroupIds.contains(groupRef) then
        locGroup = defCtx.locationGroupTable[groupRef].name;
    }

    // Initialize metrics map if needed
    ref metrics = ctx.metrics;
    if !metrics.contains(locGroup) {
      var newMetricMap = new map(string, list((real(64), OTF2_Type, OTF2_MetricValue)));
      try! {
        metrics[locGroup] = newMetricMap;
      }
    }

    var timeInSeconds = timestampToSeconds(time, defCtx.clockProps);

    // Special handling for cray_pm metrics (offset adjustment)
    const metricNameLower = metricName.toLower();
    if metricNameLower.find("craypm") >= 0 then
      timeInSeconds += ctx.evtArgs.crayTimeOffset;

    // Store metric values
    for i in 0..<numberOfMetrics {
      const valueType = typeIDs[i];
      const value = metricValues[i];

      try! {
        ref groupMetrics = metrics[locGroup];
        if !groupMetrics.contains(metricName) {
          var newList = new list((real(64), OTF2_Type, OTF2_MetricValue));
          groupMetrics[metricName] = newList;
        }
        groupMetrics[metricName].pushBack((timeInSeconds, valueType, value));
      }
    }

    return OTF2_CALLBACK_SUCCESS;
  }

  // Config constant for command-line argument
  // Usage: ./trace_to_csv_parallel --tracePath=/path/to/traces.otf2
  config const tracePath: string = "/traces/frontier-hpl-run-using-2-ranks-with-craypm/traces.otf2";

  proc main() {

    // Paths: adjust as needed
    // const tracePath = "/traces/frontier-hpl-run-using-2-ranks-with-craypm/traces.otf2";
    // const tracePath = "/Users/khandeka/dev/ornl/arkouda-telemetry-analysis/hpc-energy-trace-analysis/scorep-traces/simple-mi300-example-run/traces.otf2";
    var sw: stopwatch;
    sw.start();

    var initial_reader = OTF2_Reader_Open(tracePath.c_str());
    if initial_reader == nil {
      writeln("Failed to open trace file");
      return;
    }

    const openTime = sw.elapsed();
    writef("Time taken to open initial OTF2 archive: %.2dr seconds\n", openTime);
    sw.clear();

    OTF2_Reader_SetSerialCollectiveCallbacks(initial_reader);

    var numberOfLocations: c_uint64 = 0;
    OTF2_Reader_GetNumberOfLocations(initial_reader, c_ptrTo(numberOfLocations));
    writeln("Number of locations: ", numberOfLocations);

    // Definition context & callbacks
    var defCtx = new DefCallbackContext();
    var globalDefReader = OTF2_Reader_GetGlobalDefReader(initial_reader);
    var defCallbacks = OTF2_GlobalDefReaderCallbacks_New();
    OTF2_GlobalDefReaderCallbacks_SetClockPropertiesCallback(defCallbacks,
                                                        c_ptrTo(registerClockProperties): c_fn_ptr);
    OTF2_GlobalDefReaderCallbacks_SetStringCallback(defCallbacks, c_ptrTo(GlobDefString_Register): c_fn_ptr);
    OTF2_GlobalDefReaderCallbacks_SetLocationGroupCallback(defCallbacks, c_ptrTo(GlobDefLocationGroup_Register): c_fn_ptr);
    OTF2_GlobalDefReaderCallbacks_SetLocationCallback(defCallbacks, c_ptrTo(GlobDefLocation_Register): c_fn_ptr);
    OTF2_GlobalDefReaderCallbacks_SetRegionCallback(defCallbacks, c_ptrTo(GlobDefRegion_Register): c_fn_ptr);
    OTF2_GlobalDefReaderCallbacks_SetMetricMemberCallback(defCallbacks, c_ptrTo(GlobDefMetricMember_Register): c_fn_ptr);
    OTF2_GlobalDefReaderCallbacks_SetMetricClassCallback(defCallbacks, c_ptrTo(GlobDefMetricClass_Register): c_fn_ptr);
    OTF2_GlobalDefReaderCallbacks_SetMetricInstanceCallback(defCallbacks, c_ptrTo(GlobDefMetricInstance_Register): c_fn_ptr);
    OTF2_GlobalDefReaderCallbacks_SetMetricClassRecorderCallback(defCallbacks, c_ptrTo(GlobDefMetricClassRecorder_Register): c_fn_ptr);

    OTF2_Reader_RegisterGlobalDefCallbacks(initial_reader,
                                           globalDefReader,
                                           defCallbacks,
                                           c_ptrTo(defCtx): c_ptr(void));
    OTF2_GlobalDefReaderCallbacks_Delete(defCallbacks);

    var definitionsRead: c_uint64 = 0;
    OTF2_Reader_ReadAllGlobalDefinitions(initial_reader, globalDefReader, c_ptrTo(definitionsRead));
    writeln("Global definitions read: ", definitionsRead);

    const defReadTime = sw.elapsed();
    writef("Time taken to read global definitions: %.2dr seconds\n", defReadTime);
    sw.clear();

    // Convert associative domain to array for distribution
    const locationArray : [0..<numberOfLocations] uint = for l in defCtx.locationIds do l;
    const totalLocs = locationArray.size;
    writeln("Total locations: ", totalLocs);
    writeln("SANITY CHECK:", totalLocs == numberOfLocations);

    const locToArrayTime = sw.elapsed();
    writeln("Time taken to convert location IDs to array: ", locToArrayTime, " seconds");
    sw.clear();

    // Select locations to read definitions from, in this case, all
    for loc in locationArray {
      OTF2_Reader_SelectLocation(initial_reader, loc);
    }

    // Open files, read local defs per location
    const successfulOpenDefFiles =
                        OTF2_Reader_OpenDefFiles(initial_reader) == OTF2_SUCCESS;

    // Read all local definitions files
    for loc in locationArray {
      if successfulOpenDefFiles {
        var defReader = OTF2_Reader_GetDefReader(initial_reader, loc);
        if defReader != nil {
          var defReads: c_uint64 = 0;
          OTF2_Reader_ReadAllLocalDefinitions(initial_reader,
                                              defReader,
                                              c_ptrTo(defReads));

          OTF2_Reader_CloseDefReader(initial_reader, defReader);
        }
      }
      // No marking event files for reading as we're only doing def files for now
    }

    if successfulOpenDefFiles {
      OTF2_Reader_CloseDefFiles(initial_reader);
    }

    // Close the initial_reader now that we have the number of locations
    // and definitions
    OTF2_Reader_Close(initial_reader);

    // This is to use the most number of readers that makes sense
    // const numberOfReaders = 5;
    const numberOfReaders = here.maxTaskPar;
    writeln("Number of readers: ", numberOfReaders);

    // Metrics to track
    var metricsToTrack: domain(string) = {
    'A2rocm_smi:::energy_count:device=0',
    'A2rocm_smi:::energy_count:device=2',
    'A2rocm_smi:::energy_count:device=4',
    'A2rocm_smi:::energy_count:device=6',

    'A2coretemp:::craypm:accel0_energy',
    'A2coretemp:::craypm:accel1_energy',
    'A2coretemp:::craypm:accel2_energy',
    'A2coretemp:::craypm:accel3_energy',
    };

    // Empty for now since I don't know how to populate this in Chapel
    var processesToTrack: domain(string);

    var crayPmOffset: real(64) = 1.0;
    var evtArgs = new EvtCallbackArgs(processesToTrack=processesToTrack,
                                      metricsToTrack=metricsToTrack,
                                      crayTimeOffset=crayPmOffset);

    var totalEventsReadAcrossReaders: c_uint64 = 0;

    // Allocate per-reader event contexts that we'll merge after parallel region
    var evtContexts: [0..<numberOfReaders] EvtCallbackContext;

    coforall i in 0..<numberOfReaders with (+ reduce totalEventsReadAcrossReaders, ref defCtx, ref evtContexts) {
      // Each task will have its own reader
      var reader = OTF2_Reader_Open(tracePath.c_str());

      if reader != nil {
        OTF2_Reader_SetSerialCollectiveCallbacks(reader);

        var sw_inner: stopwatch;
        sw_inner.start();

        var numLocationsToReadForThisTask = totalLocs / numberOfReaders;
        const low = i * numLocationsToReadForThisTask;
        const high = if i == numberOfReaders - 1 then totalLocs
                     else (i + 1) * numLocationsToReadForThisTask;

        // Select locations for this task
        for locIdx in low..<high {
          const loc = locationArray[locIdx];
          // writeln("Task ", i, " selecting location ", loc);
          OTF2_Reader_SelectLocation(reader, loc);
        }

        OTF2_Reader_OpenEvtFiles(reader);

        for locIdx in low..<high {
          const loc = locationArray[locIdx];
          // Mark file to be read by Global Reader later
          var _evtReader = OTF2_Reader_GetEvtReader(reader, loc);
        }

        const markTime = sw_inner.elapsed();
        writeln("Time taken to mark all local event files for reading (task ", i, "): ", markTime, " seconds");
        sw_inner.clear();

        var globalEvtReader = OTF2_Reader_GetGlobalEvtReader(reader);
        var evtCallbacks = OTF2_GlobalEvtReaderCallbacks_New();
        // Local context for this task; copied into shared array after reading events
        var localEvtCtx = new EvtCallbackContext(evtArgs, defCtx);
        ref evtCtx = localEvtCtx;

        OTF2_GlobalEvtReaderCallbacks_SetEnterCallback(evtCallbacks,
                                                      c_ptrTo(Enter_callback): c_fn_ptr);
        OTF2_GlobalEvtReaderCallbacks_SetLeaveCallback(evtCallbacks,
                                                      c_ptrTo(Leave_callback): c_fn_ptr);
        OTF2_GlobalEvtReaderCallbacks_SetMetricCallback(evtCallbacks,
                                                        c_ptrTo(Metric_callback): c_fn_ptr);

        OTF2_Reader_RegisterGlobalEvtCallbacks(reader,
                                              globalEvtReader,
                                              evtCallbacks,
                                              c_ptrTo(evtCtx): c_ptr(void));

        OTF2_GlobalEvtReaderCallbacks_Delete(evtCallbacks);

        var totalEventsRead: c_uint64 = 0;
        OTF2_Reader_ReadAllGlobalEvents(reader,
                                        globalEvtReader,
                                        c_ptrTo(totalEventsRead));
        totalEventsReadAcrossReaders += totalEventsRead;

        const evtReadTime = sw_inner.elapsed();
        writeln("Time taken to read events (task ", i, "): ", evtReadTime, " seconds");
        sw_inner.clear();
        OTF2_Reader_CloseGlobalEvtReader(reader, globalEvtReader);
        OTF2_Reader_CloseEvtFiles(reader);
        OTF2_Reader_Close(reader);
        const closeTime = sw_inner.elapsed();
        sw_inner.stop();
        sw_inner.clear();
        // Copy local context with accumulated events into global array slot
        evtContexts[i] = localEvtCtx;
      } else {
        writeln("Failed to open trace file");
      }
    }
    sw.stop();
    writeln("Total time: ", sw.elapsed(), " seconds");

    // --- Merge per-reader contexts into a single aggregated structure ---
    writeln("\n--- Merging contexts from parallel readers ---");

    var mergedEvtCtx = new EvtCallbackContext(evtArgs, defCtx);

    for i in 0..<numberOfReaders {
      const ctx = evtContexts[i];

      // Merge seenGroups
      for (group, threads) in ctx.seenGroups.items() {
        if !mergedEvtCtx.seenGroups.contains(group) {
          try! mergedEvtCtx.seenGroups[group] = threads;
        } else {
          try! mergedEvtCtx.seenGroups[group] += threads;
        }
      }

      // Merge callGraphs
      for (group, threadMap) in ctx.callGraphs.items() {
        if !mergedEvtCtx.callGraphs.contains(group) {
          try! mergedEvtCtx.callGraphs[group] = threadMap;
        } else {
          for (thread, callGraph) in threadMap.items() {
            if !mergedEvtCtx.callGraphs[group].contains(thread) {
              try! mergedEvtCtx.callGraphs[group][thread] = callGraph;
            } else {
              // If both have same thread, we need to merge call graphs
              // For simplicity, we can keep the existing one (shouldn't happen with proper partitioning)
              writeln("Warning: Duplicate thread ", thread, " in group ", group, " - keeping first occurrence");
            }
          }
        }
      }

      // Merge metrics
      for (group, metricMap) in ctx.metrics.items() {
        if !mergedEvtCtx.metrics.contains(group) {
          try! mergedEvtCtx.metrics[group] = metricMap;
        } else {
          for (metricName, values) in metricMap.items() {
            if !mergedEvtCtx.metrics[group].contains(metricName) {
              try! mergedEvtCtx.metrics[group][metricName] = values;
            } else {
              // Append values from this reader
              for val in values {
                try! mergedEvtCtx.metrics[group][metricName].pushBack(val);
              }
            }
          }
        }
      }
    }

    writeln("Merge complete. Total events read: ", totalEventsReadAcrossReaders);
    
    // printCallGraphAndMetrics(mergedEvtCtx, true);
    writeCallGraphsAndMetricsToCSV(mergedEvtCtx);
  }

  proc callgraphToCSV(callGraph: shared CallGraph, group: string, thread: string, filename: string) {
    // Convert a CallGraph to a CSV file
    try {
      var outfile = open(filename, ioMode.cw);
      var writer = outfile.writer(locking=false);

      writer.writeln("Thread,Group,Depth,Name,Start Time,End Time,Duration");

      const intervals = callGraph.getIntervalsBetween(-inf, inf);

      for iv in intervals {
        const start = iv.start;
        const end = if iv.hasEnd then iv.end else inf;
        const duration = end - start;
        const name = if iv.name != "" then iv.name else "Unknown";
        const depth = iv.depth;

        writer.writef("%s,%s,%i,\"%s\",%.15dr,%.15dr,%.15dr\n",
                      thread, group, depth, name, start, end, duration);
      }

      writer.close();
      outfile.close();
    } catch e {
      writeln("Error writing callgraph to CSV: ", e);
    }
  }

  proc metricsToCSV(group: string, threadMetrics: map(string, list((real(64), OTF2_Type, OTF2_MetricValue))), filename: string) {
    // Convert metrics to a CSV file
    // Note: In the Python version, metrics are stored as List[Tuple[float, float]] (time, value)
    try {
      var outfile = open(filename, ioMode.cw);
      var writer = outfile.writer(locking=false);

      writer.writeln("Group,Metric Name,Time,Value");

      for (metricName, values) in threadMetrics.items() {
        for (time, valueType, value) in values {
          if valueType == OTF2_TYPE_INT64 then
            writer.writef("%s,%s,%.15dr,%i\n", group, metricName, time, value.signed_int);
          else if valueType == OTF2_TYPE_UINT64 then
            writer.writef("%s,%s,%.15dr,%u\n", group, metricName, time, value.unsigned_int);
          else if valueType == OTF2_TYPE_DOUBLE then
            writer.writef("%s,%s,%.15dr,%.15dr\n", group, metricName, time, value.floating_point);
        }
      }

      writer.close();
      outfile.close();
    } catch e {
      writeln("Error writing metrics to CSV: ", e);
    }
  }

  proc writeCallGraphsAndMetricsToCSV(evtCtx: EvtCallbackContext) {
    // Write call graphs to CSV files

    // cobegin {
    forall (group, threads) in evtCtx.callGraphs.toArray() {
      if !evtCtx.evtArgs.processesToTrack.isEmpty() && !evtCtx.evtArgs.processesToTrack.contains(group) {
        writeln("Skipping group ", group, " as it is not in the processes to track.");
        continue;
      }
      forall thread in threads.keysToArray() {
        const callGraph = try! threads[thread];
        const filename = group + "_" + thread.replace(" ", "_") + "_callgraph.csv";
        writeln("Writing to file: ", filename);
        callgraphToCSV(callGraph, group, thread, filename);
      }
    }

    // Write metrics to CSV files
    forall (group, threadMetrics) in evtCtx.metrics.toArray() {
      if !evtCtx.evtArgs.processesToTrack.isEmpty() && !evtCtx.evtArgs.processesToTrack.contains(group) {
        writeln("Skipping group ", group, " as it is not in the processes to track.");
        continue;
      }
      const filename = group + "_metrics.csv";
      writeln("Writing to file: ", filename);
      metricsToCSV(group, threadMetrics, filename);
    }
    // }
  }

  proc printCallGraphAndMetrics(evtCtx: EvtCallbackContext, verbose: bool = false) {
    // Output call graphs and metrics summary to console
    writeln("\n--- Call Graphs ---");
    writeln("Total location groups with call graphs: ", evtCtx.callGraphs.size);
    for (locGroup, locMap) in evtCtx.callGraphs.items() {
      writeln("Location Group: ", locGroup);
      for (locName, callGraph) in locMap.items() {
        writeln("  Thread: ", locName);
      }
    }

    writeln("\n--- Metrics Summary ---");
    var totalMetricsStored: int = 0;
    for (locGroup, metricMap) in evtCtx.metrics.items() {
      writeln("Location Group: ", locGroup);
      for (metricName, values) in metricMap.items() {
        write("  Metric: ", metricName, ", Count: ", values.size);
        if values.size > 0 {
          writeln(", First Value: ", values[0]);
        } else {
          writeln();
        }
        totalMetricsStored += values.size;
      }
    }
    writeln("Total metrics stored: ", totalMetricsStored);

  }
}
