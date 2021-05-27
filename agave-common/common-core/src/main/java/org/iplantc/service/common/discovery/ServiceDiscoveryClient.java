package org.iplantc.service.common.discovery;

import org.iplantc.service.common.exceptions.ServiceDiscoveryException;

import java.util.List;

public interface ServiceDiscoveryClient<A extends PlatformService<?>, W extends PlatformService<?>>
{
	List<A> listApiServicesWithCapability(ServiceCapability capability) throws ServiceDiscoveryException;

	void addApiService(A discoverableApi) throws ServiceDiscoveryException;

	void deleteApiService(A discoverableApi) throws ServiceDiscoveryException;

	List<A> listApiServices() throws ServiceDiscoveryException;

	List<W> listWorkerServiceWithCapability(ServiceCapability capability) throws ServiceDiscoveryException;

	void addDiscoverableWorker(W discoverableWorker) throws ServiceDiscoveryException;

	void deleteWorkerService(W discoverableWorker) throws ServiceDiscoveryException;

	List<W> listWorkerServices() throws ServiceDiscoveryException;

}