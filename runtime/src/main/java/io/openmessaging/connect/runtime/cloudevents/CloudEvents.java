package io.openmessaging.connect.runtime.cloudevents;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

public interface CloudEvents<T> {

    /**
     * Type of occurrence which has happened. Often this property is used for routing, observability, policy enforcement, etc.
     */
    String getType();

    /**
     * The version of the CloudEvents specification which the event uses. This enables the interpretation of the context.
     */
    String getSpecVersion();

    /**
     * This describes the event producer. Often this will include information such as the type of the event source, the organization publishing the event, and some unique identifiers.
     * The exact syntax and semantics behind the data encoded in the URI is event producer defined.
     */
    URI getSource();

    /**
     * ID of the event. The semantics of this string are explicitly undefined to ease the implementation of producers. Enables deduplication.
     */
    String getId();

    /**
     * Timestamp of when the event happened.
     */
    Optional<ZonedDateTime> getTime();

    /**
     * A link to the schema that the data attribute adheres to.
     */
    Optional<URI> getSchemaURL();

    /**
     * Describe the data encoding format
     */
    Optional<String> getContentType();

    /**
     * The event payload. The payload depends on the eventType, schemaURL and eventTypeVersion, the payload is encoded into a media format which is specified by the contentType attribute (e.g. application/json).
     */
    Optional<T> getData();

    /**
     *
     */
    Optional<List<Extension>> getExtensions();
}
