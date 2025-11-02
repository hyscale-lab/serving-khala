package net

// // baseline activator version
// func (rt *revisionThrottler) try(ctx context.Context, function func(string) error) error {
// 	var ret error

// 	// Retrying infinitely as long as we receive no dest. Outer semaphore and inner
// 	// pod capacity are not changed atomically, hence they can race each other. We
// 	// "reenqueue" requests should that happen.
// 	reenqueue := true
// 	for reenqueue {
// 		reenqueue = false
// 		if err := rt.breaker.Maybe(ctx, func() {
// 			cb, tracker := rt.acquireDest(ctx)
// 			if tracker == nil {
// 				// This can happen if individual requests raced each other or if pod
// 				// capacity was decreased after passing the outer semaphore.
// 				reenqueue = true
// 				return
// 			}
// 			defer cb()
// 			// We already reserved a guaranteed spot. So just execute the passed functor.
// 			ret = function(tracker.dest)
// 		}); err != nil {
// 			return err
// 		}
// 	}
// 	return ret
// }

// // exponential backoff version
// func (rt *revisionThrottler) try(ctx context.Context, function func(string) error) error {
// 	var ret error
// 	var err error
// 	var vm *activator.VMMetadata

// 	// Pop out a VM for this request
// 	vm = rt.vmList.PopVM()
// 	if vm == nil {
// 		rt.logger.Infof("khala: no available VM. creating a new one")
// 		// No VM available, need to create a new one
// 		// Create a new VM for this request
// 		// try create VM 5 times with exponential backoff 10 20 40 80 160 ms
// 		for retryAttempts := 0; retryAttempts < 5; retryAttempts++ {
// 			time.Sleep(time.Duration(10*(1<<retryAttempts)) * time.Millisecond)
// 			vm = rt.vmList.PopVM()
// 			if vm != nil {
// 				break
// 			}
// 			rt.logger.Errorf("khala: failed to create VM: %v", err)
// 		}
// 		if vm == nil {
// 			vm, err = rt.vmList.CreateVM()
// 			if err != nil {
// 				rt.logger.Errorf("khala: failed to create VM: %v", err)
// 				return err
// 			}
// 		}
// 	}

// 	defer func() {
// 		if ret != nil {
// 			rt.vmList.InvalidateVM(vm)
// 		} else {
// 			rt.vmList.PushVM(vm, true)
// 		}
// 	}()

// 	ret = function(vm.Node + ":" + vm.RPCPort)

// 	return ret
// }

// // synchronous implementation
// func (rt *revisionThrottler) try(ctx context.Context, function func(string) error) error {
// 	var ret error
// 	var err error
// 	var vm *activator.VMMetadata

// 	// Pop out a VM for this request
// 	vm = rt.vmList.PopVM()
// 	if vm == nil {
// 		rt.logger.Infof("khala: no available VM. creating a new one")
// 		vm, err = rt.vmList.CreateVM()
// 		if err != nil {
// 			rt.logger.Errorf("khala: failed to create VM: %v", err)
// 			return err
// 		}
// 	}

// 	defer func() {
// 		if ret != nil {
// 			rt.vmList.InvalidateVM(vm)
// 		} else {
// 			rt.vmList.PushVM(vm, true)
// 		}
// 	}()

// 	ret = function(vm.Node + ":" + vm.RPCPort)

// 	return ret
// }

// // queue up request for each revision throttler.
// // reuse breaker used in baseline.
// func (rt *revisionThrottler) try(ctx context.Context, function func(string) error) error {
// 	var ret error

// 	err := rt.khalaBreaker.Maybe(ctx, func() {
// 		var vm *khala.VMMetadata
// 		for {
// 			// First, try to get an available VM.
// 			vm = rt.vmList.PopVM()
// 			if vm != nil {
// 				break
// 			}

// 			// If there are no VMs and we are not already creating one, trigger a creation.
// 			if !rt.creatingVM.Load() {
// 				rt.creatingVM.Store(true)
// 				go func() {
// 					defer rt.creatingVM.Store(false)
// 					newVM, err := rt.vmList.CreateVM()
// 					if err != nil {
// 						rt.logger.Errorf("khala: failed to create VM: %v", err)
// 						return
// 					}
// 					rt.vmList.PushVM(newVM, true)
// 				}()
// 			}

// 			// Wait for a VM to become available.
// 			select {
// 			case <-ctx.Done():
// 				ret = ctx.Err()
// 				return
// 			case <-time.After(10 * time.Millisecond):
// 				// Continue loop to try PopVM again.
// 			}
// 		}

// 		// Once we have a VM, execute the function.
// 		defer func() {
// 			if ret != nil {
// 				rt.vmList.InvalidateVM(vm)
// 			} else {
// 				rt.vmList.PushVM(vm, true)
// 			}
// 		}()
// 		ret = function(vm.Node + ":" + vm.RPCPort)
// 	})

// 	if err != nil {
// 		return err
// 	}
// 	return ret
// }
