/*
Copyright 2020 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package activator

import (
	"os"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"knative.dev/serving/pkg/autoscaler/metrics"
)

// RawSender sends raw byte array messages with a message type
// (implemented by gorilla/websocket.Socket).
type RawSender interface {
	SendRaw(msgType int, msg []byte) error
}

// ReportStats sends any messages received on the source channel to the sink.
// The messages are sent on a goroutine to avoid blocking, which means that
// messages may arrive out of order.
func ReportStats(logger *zap.SugaredLogger, sink RawSender, source <-chan []metrics.StatMessage) {
	for sms := range source {
		go func(sms []metrics.StatMessage) {
			wsms := metrics.ToWireStatMessages(sms)
			// b, err := wsms.Marshal()
			_, err := wsms.Marshal()
			if err != nil {
				logger.Errorw("Error while marshaling stats", zap.Error(err))
				return
			}

			// if err := sink.SendRaw(websocket.BinaryMessage, b); err != nil {
			// 	logger.Errorw("Error while sending stats", zap.Error(err))
			// }
		}(sms)
	}
}

// VMRequestInfo holds information about a VM's IP address and the time of the last request.
// This struct is useful for tracking when a VM was last used and what its IP address is.
type VMRequestInfo struct {
	IPAddress     string // The IP address of the VM
	LastRequestAt int64  // The Unix timestamp
	EndTime       int64  // The Unix timestamp
	inUse         bool   // Whether the VM is currently in use
}

// VMRequestHistory is a global, exportable map that tracks the request history for each VM by name.
// The key is the VM name (string), and the value is a slice (array) of VMRequestInfo structs.
var VMRequestHistory = make(map[string][]VMRequestInfo)
var VMRequestHistoryMutex sync.RWMutex

func GetEnv(key string, fallback int) int {
	if value, ok := os.LookupEnv(key); ok {
		i, err := strconv.Atoi(value)
		if err != nil {
			return fallback
		}
		return i
	}
	return fallback
}

func GetNotInUseVMs(name string) []VMRequestInfo {
	VMRequestHistoryMutex.Lock()
        defer VMRequestHistoryMutex.Unlock()
	vmRequestHistory := VMRequestHistory[name]
	notInUseVMs := []VMRequestInfo{}
	for _, vmRequestInfo := range vmRequestHistory {
		if !vmRequestInfo.inUse {
			notInUseVMs = append(notInUseVMs, vmRequestInfo)
		}
	}
	return notInUseVMs
}

func SetLastRequestAt(name string, ipAddress string, lastRequestAt int64) {
	VMRequestHistoryMutex.Lock()
	defer VMRequestHistoryMutex.Unlock()
	vmRequestHistory := VMRequestHistory[name]
	for _, vmRequestInfo := range vmRequestHistory {
		if vmRequestInfo.IPAddress == ipAddress {
			vmRequestInfo.LastRequestAt = lastRequestAt
		}
	}
}

func SetInUse(name string, ipAddress string, inUse bool) {
	VMRequestHistoryMutex.Lock()
	defer VMRequestHistoryMutex.Unlock()
	vmRequestHistory := VMRequestHistory[name]
	for _, vmRequestInfo := range vmRequestHistory {
		if vmRequestInfo.IPAddress == ipAddress {
			vmRequestInfo.inUse = inUse
			if inUse {
				vmRequestInfo.EndTime = 0
			} else {
				vmRequestInfo.EndTime = time.Now().UnixMilli()
			}
		}
	}
}

func AddVMRequestHistory(name string, ipAddress string, lastRequestAt int64) {
	VMRequestHistoryMutex.Lock()
        defer VMRequestHistoryMutex.Unlock()
	vmRequestHistory := VMRequestHistory[name]
	vmRequestHistory = append(vmRequestHistory, VMRequestInfo{
		IPAddress:     ipAddress,
		LastRequestAt: lastRequestAt,
		inUse:         true,
		EndTime:       0,
	})
	VMRequestHistory[name] = vmRequestHistory
}

func RemoveVMRequestHistory(name string, ipAddress string) {
	VMRequestHistoryMutex.Lock()
        defer VMRequestHistoryMutex.Unlock()
	vmRequestHistory := VMRequestHistory[name]
	for i, vmRequestInfo := range vmRequestHistory {
		if vmRequestInfo.IPAddress == ipAddress {
			vmRequestHistory = append(vmRequestHistory[:i], vmRequestHistory[i+1:]...)
		}
	}
	VMRequestHistory[name] = vmRequestHistory
}

// RemoveVMRequestHistoryByIP removes a VM entry from the VMRequestHistory map by its IP address.
// This function searches through all VMRequestHistory entries for all names and removes any VM whose IPAddress matches the given ipAddress.
// This is useful for cleaning up VMs that have been deleted or are no longer needed.
func RemoveVMRequestHistoryByIP(ipAddress string) {
	VMRequestHistoryMutex.Lock()
	defer VMRequestHistoryMutex.Unlock()
	for name, vmRequestHistory := range VMRequestHistory {
		// Create a new slice to hold VMs that do not match the given IP address
		var filteredVMs []VMRequestInfo
		for _, vmRequestInfo := range vmRequestHistory {
			if vmRequestInfo.IPAddress != ipAddress {
				filteredVMs = append(filteredVMs, vmRequestInfo)
			}
		}
		// Update the VMRequestHistory for this name
		VMRequestHistory[name] = filteredVMs
	}
}

// SetOldestVMNotInUse sets the 'inUse' field to false for the oldest VM (with the earliest LastRequestAt)
// for a given name. If there are no VMs for the given name, nothing happens.
func SetOldestVMNotInUse(name string, logger *zap.SugaredLogger) {
	VMRequestHistoryMutex.Lock()
	defer VMRequestHistoryMutex.Unlock()
	vmRequestHistory := VMRequestHistory[name]
	if len(vmRequestHistory) == 0 {
		// No VMs to update, so just return.
		return
	}

	// Find the index of the VM with the oldest (smallest) LastRequestAt.
	oldestIdx := 0
	oldestTime := vmRequestHistory[0].LastRequestAt

	// Only get it for between inUse VMs
	for i, vmRequestInfo := range vmRequestHistory {
		if vmRequestInfo.inUse {
			oldestTime = vmRequestInfo.LastRequestAt
			oldestIdx = i
			break
		}
	}

	for i, vmRequestInfo := range vmRequestHistory {
		if vmRequestInfo.LastRequestAt < oldestTime && vmRequestInfo.inUse {
			oldestTime = vmRequestInfo.LastRequestAt
			oldestIdx = i
		}
	}
	logger.Infof("khala: vm_index: %v", oldestIdx)

	// Set the inUse field to false for the oldest VM.
	vmRequestHistory[oldestIdx].inUse = false
	vmRequestHistory[oldestIdx].EndTime = time.Now().UnixMilli()

	// Update the VMRequestHistory map with the modified slice.
	VMRequestHistory[name] = vmRequestHistory
}

var ToRemoveVMs = []VMRequestInfo{}

// RemoveVMsWithLongEndTime removes all VMs for a given name whose EndTime is at least 40 seconds in the future compared to the current time.
// This function helps to clean up VMs that are not supposed to be active for much longer.
//
// name: the key for which to check the VMRequestHistory map.
func RemoveVMsWithLongEndTime(name string) {
	VMRequestHistoryMutex.Lock()
	defer VMRequestHistoryMutex.Unlock()
	// Get the current time as a Unix timestamp (seconds since Jan 1, 1970)
	now := time.Now().UnixMilli()

	// Get the VM request history for the given name
	vmRequestHistory := VMRequestHistory[name]

	// Create a new slice to hold VMs that should be kept
	var filteredVMs []VMRequestInfo

	for _, vm := range vmRequestHistory {
		// If EndTime is zero, it means the VM request hasn't ended yet, so we keep it
		// If EndTime is not zero, check if it's more than 2 minutes from now
		keepalive_duration := GetEnv("KEEPALIVE_DURATION", 2*60)
		if vm.EndTime == 0 || now-vm.EndTime < int64(keepalive_duration)*1000 {
			// Keep this VM
			filteredVMs = append(filteredVMs, vm)
		} else {
			ToRemoveVMs = append(ToRemoveVMs, vm)
		}
	}

	// Update the VMRequestHistory map with the filtered list
	VMRequestHistory[name] = filteredVMs
}
