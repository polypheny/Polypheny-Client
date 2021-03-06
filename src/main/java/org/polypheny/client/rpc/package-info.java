/**
 * This package is responsible for the network-communication. It contains the implementations of the GRPC-Services, Wrappers and utilities to generate proto-objects. If you change something in the protobuf-definition, please update the corresponding generator in the {@link
 * org.polypheny.client.rpc.ProtoObjectFactory}. We separate Master-stuff and worker-stuff into two packages.
 *
 * @author Silvan Heller
 */

package org.polypheny.client.rpc;