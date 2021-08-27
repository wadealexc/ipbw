package crawler

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wadeAlexC/ipbw/crawler/types"
)

type DHTStats struct {
	startTime time.Time

	totalInbound uint64

	readMu         sync.Mutex
	totalReads     uint64
	totalReadSize  uint64
	readsByMsgType map[int32]uint64

	writeMu         sync.Mutex
	totalWrites     uint64
	totalWriteSize  uint64
	writesByMsgType map[int32]uint64

	infoMu           sync.Mutex
	peersIdentified  uint64
	protocols        map[string]uint64
	protocolVersions map[string]uint64
	userAgents       map[string]uint64

	errMu           sync.Mutex
	workerErrs      map[string]uint64 // Errors emitted from reads/writes
	unreachableErrs map[string]uint64 // Errors emitted when we attempt connection
}

func NewDHTStats() *DHTStats {
	return &DHTStats{
		readsByMsgType:   make(map[int32]uint64),
		writesByMsgType:  make(map[int32]uint64),
		protocols:        make(map[string]uint64),
		protocolVersions: make(map[string]uint64),
		userAgents:       make(map[string]uint64),
		workerErrs:       make(map[string]uint64),
		unreachableErrs:  make(map[string]uint64),
	}
}

func (st *DHTStats) setStartTime() {
	st.startTime = time.Now()
}

func (st *DHTStats) getTimeElapsed() time.Duration {
	return time.Since(st.startTime)
}

func (st *DHTStats) logRead(p *types.Peer, mType types.MessageType, mSize int) {
	st.readMu.Lock()
	defer st.readMu.Unlock()

	st.totalReads++
	st.totalReadSize += uint64(mSize)
	st.readsByMsgType[int32(mType)]++
}

func (st *DHTStats) logWrite(p *types.Peer, mType types.MessageType, mSize int) {
	st.writeMu.Lock()
	defer st.writeMu.Unlock()

	st.totalWrites++
	st.totalWriteSize += uint64(mSize)
	st.writesByMsgType[int32(mType)]++
}

func (st *DHTStats) logIdentify(p *types.Peer, protos []string, protoVersion string, agent string) {
	st.infoMu.Lock()
	defer st.infoMu.Unlock()

	// Tracks the protocols we've seen so we don't
	// count any twice
	allProtos := map[string]struct{}{}

	for _, proto := range protos {
		// Duplicate protocol! Skip.
		if _, seen := allProtos[proto]; seen {
			continue
		}

		allProtos[proto] = struct{}{}
		st.protocols[proto]++
	}

	st.protocolVersions[protoVersion]++
	st.userAgents[agent]++
	st.peersIdentified++
}

func (st *DHTStats) logInboundConn(p *types.Peer) {
	atomic.AddUint64(&st.totalInbound, 1)
}

func (st *DHTStats) logError(p *types.Peer, err error) {
	st.errMu.Lock()
	defer st.errMu.Unlock()

	st.workerErrs[err.Error()]++
}

func (st *DHTStats) logUnreachable(p *types.Peer, err error) {
	st.errMu.Lock()
	defer st.errMu.Unlock()

	st.unreachableErrs[err.Error()]++
}

func (st *DHTStats) printStats() {
	strs := getHeader("DETAILED CRAWL STATS")

	strs = append(strs, fmt.Sprintf("Total inbound connections: %d", atomic.LoadUint64(&st.totalInbound)))
	strs = append(strs, getFooter())

	strs = append(strs, st.getReadStats()...)
	strs = append(strs, getFooter())

	strs = append(strs, st.getWriteStats()...)
	strs = append(strs, getFooter())

	strs = append(strs, st.getPeerStats()...)
	strs = append(strs, getFooter())

	strs = append(strs, st.getErrStats()...)
	strs = append(strs, getFooter())

	fmt.Println(strings.Join(strs, "\n"))
}

func (st *DHTStats) getReadStats() []string {
	st.readMu.Lock()
	defer st.readMu.Unlock()

	strs := []string{}

	strs = append(strs, fmt.Sprintf("READ STATS:"))
	strs = append(strs, fmt.Sprintf("Total bytes read: %d", st.totalReadSize))
	strs = append(strs, fmt.Sprintf("Messages read: %d; by type:", st.totalReads))
	strs = append(strs, fmt.Sprintf("- PUT_VALUE: %d", st.readsByMsgType[int32(types.PUT_VALUE)]))
	strs = append(strs, fmt.Sprintf("- GET_VALUE: %d", st.readsByMsgType[int32(types.GET_VALUE)]))
	strs = append(strs, fmt.Sprintf("- ADD_PROVIDER: %d", st.readsByMsgType[int32(types.ADD_PROVIDER)]))
	strs = append(strs, fmt.Sprintf("- GET_PROVIDERS: %d", st.readsByMsgType[int32(types.GET_PROVIDERS)]))
	strs = append(strs, fmt.Sprintf("- FIND_NODE: %d", st.readsByMsgType[int32(types.FIND_NODE)]))
	strs = append(strs, fmt.Sprintf("- PING: %d", st.readsByMsgType[int32(types.PING)]))

	// If we somehow got messages outside the normal range, add those here
	for msgType, count := range st.readsByMsgType {
		if types.IsKnownMsgType(msgType) {
			continue
		}

		strs = append(strs, fmt.Sprintf("- UNKNOWN(%d): %d", msgType, count))
	}

	return strs
}

func (st *DHTStats) getWriteStats() []string {
	st.writeMu.Lock()
	defer st.writeMu.Unlock()

	strs := []string{}

	strs = append(strs, fmt.Sprintf("WRITE STATS:"))
	strs = append(strs, fmt.Sprintf("Total bytes written: %d", st.totalWriteSize))
	strs = append(strs, fmt.Sprintf("Messages written: %d; by type:", st.totalWrites))
	strs = append(strs, fmt.Sprintf("- FIND_NODE: %d", st.writesByMsgType[int32(types.FIND_NODE)]))
	strs = append(strs, fmt.Sprintf("- PING: %d", st.writesByMsgType[int32(types.PING)]))

	return strs
}

type prefixInfo struct {
	prefix     string
	totalCount uint64
	names      []*uniqueName
}

type uniqueName struct {
	name  string
	count uint64
}

func (st *DHTStats) getPeerStats() []string {
	st.infoMu.Lock()
	defer st.infoMu.Unlock()

	strs := []string{}

	strs = append(strs, fmt.Sprintf("PEER STATS:"))
	strs = append(strs, fmt.Sprintf("Total peers identified: %d", st.peersIdentified))

	// Display protocols by prefix, sorted by total peers supporting
	// e.g. all unique "/ipfs" protos are grouped together, sorted
	// by number of peers supporting each
	strs = append(strs, fmt.Sprintf("Total unique protocols: %d; by type:", len(st.protocols)))
	byPrefix := splitByPrefix(st.protocols)
	for _, pInfo := range byPrefix {
		strs = append(strs, fmt.Sprintf("- Peers supporting %s:", pInfo.prefix))

		outCount := 0
		for _, proto := range pInfo.names {
			strs = append(strs, fmt.Sprintf("--- %s: %d", proto.name, proto.count))
			// Limit output for each to 5 to reduce clutter
			outCount++
			if outCount >= 5 && len(pInfo.names) > outCount {
				strs = append(strs, fmt.Sprintf("...trimmed %d additional %s protocols", len(pInfo.names)-outCount, pInfo.prefix))
				break
			}
		}

		strs = append(strs, "")
	}

	strs = append(strs, getFooter())

	// Display protocol versions by prefix, sorted by total peers supporting
	strs = append(strs, fmt.Sprintf("Total unique protocol versions: %d; by type:", len(st.protocolVersions)))
	byPrefix = splitByPrefix(st.protocolVersions)
	for _, pInfo := range byPrefix {
		strs = append(strs, fmt.Sprintf("- Peers using %s:", pInfo.prefix))

		outCount := 0
		for _, proto := range pInfo.names {
			strs = append(strs, fmt.Sprintf("--- %s: %d", proto.name, proto.count))
			// Limit output for each to 5 to reduce clutter
			outCount++
			if outCount >= 5 && len(pInfo.names) > outCount {
				strs = append(strs, fmt.Sprintf("...trimmed %d additional %s versions", len(pInfo.names)-outCount, pInfo.prefix))
				break
			}
		}

		strs = append(strs, "")
	}

	strs = append(strs, getFooter())

	// Display user agents by prefix, sorted by total peers
	strs = append(strs, fmt.Sprintf("Total unique user agents: %d; by type:", len(st.userAgents)))
	byPrefix = splitByPrefix(st.userAgents)
	for _, pInfo := range byPrefix {
		strs = append(strs, fmt.Sprintf("- Peers using %s:", pInfo.prefix))

		outCount := 0
		for _, proto := range pInfo.names {
			strs = append(strs, fmt.Sprintf("--- %s: %d", proto.name, proto.count))
			// Limit output for each to 5 to reduce clutter
			outCount++
			if outCount >= 5 && len(pInfo.names) > outCount {
				strs = append(strs, fmt.Sprintf("...trimmed %d additional %s user agents", len(pInfo.names)-outCount, pInfo.prefix))
				break
			}
		}

		strs = append(strs, "")
	}

	strs = append(strs, getFooter())

	return strs
}

func (st *DHTStats) getErrStats() []string {
	st.errMu.Lock()
	defer st.errMu.Unlock()

	strs := []string{}

	strs = append(strs, fmt.Sprintf("ERROR STATS:"))

	strs = append(strs, fmt.Sprintf("Total unique read/write errors: %d; by type:", len(st.workerErrs)))
	for err, count := range st.workerErrs {
		strs = append(strs, fmt.Sprintf("- %s: %d", err, count))
	}

	strs = append(strs, fmt.Sprintf("Total unique connection errors: %d", len(st.unreachableErrs)))
	// for err, count := range st.unreachableErrs {
	// 	strs = append(strs, fmt.Sprintf("- %s: %d", err, count))
	// }

	return strs
}

func splitByPrefix(m map[string]uint64) []*prefixInfo {
	// Split output by name prefix. e.g: all protocols
	// that start with "/ipfs" will be displayed together
	pfxInfos := make([]*prefixInfo, 0)
	seenPrefixes := make(map[string]int)
	for name, count := range m {
		prefix := getPrefix(name)
		unique := &uniqueName{
			name:  name,
			count: count,
		}

		// If we've seen this prefix, add to the total count
		// Otherwise, create a new prefixInfo
		if idx, seen := seenPrefixes[prefix]; seen {
			pfxInfos[idx].totalCount += count
			pfxInfos[idx].names = append(pfxInfos[idx].names, unique)
		} else {
			seenPrefixes[prefix] = len(pfxInfos)
			pfxInfos = append(pfxInfos, &prefixInfo{
				prefix:     prefix,
				totalCount: count,
				names:      []*uniqueName{unique},
			})
		}
	}

	// Sort prefix infos by totalCount
	sort.Slice(pfxInfos, func(i, j int) bool {
		return pfxInfos[i].totalCount > pfxInfos[j].totalCount
	})

	// For each prefixInfo, sort its names by count
	for _, pInfo := range pfxInfos {
		sort.Slice(pInfo.names, func(i, j int) bool {
			return pInfo.names[i].count > pInfo.names[j].count
		})
	}

	return pfxInfos
}

func getPrefix(proto string) string {
	strs := strings.Split(proto, "/")

	// Shouldn't happen unless the proto string is empty
	if len(strs) == 0 {
		return "NONE"
	}

	// Proto string did not contain "/"
	if len(strs) == 1 {
		return strs[0]
	}

	// Proto string contained "/" at beginning
	if strs[0] == "" {
		return strs[1]
	}

	// Proto string contained "/", but not at beginning
	// (This is unusual)
	return strs[0]
}
