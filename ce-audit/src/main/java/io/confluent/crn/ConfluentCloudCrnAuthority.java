package io.confluent.crn;

import io.confluent.crn.ConfluentResourceName.Builder;
import io.confluent.crn.ConfluentResourceName.Element;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.Scope;
import java.util.List;

public class ConfluentCloudCrnAuthority extends ConfluentServerCrnAuthority {

    public static final String AUTHORITY_NAME = "confluent.cloud";
    public static final String PATH_TYPE_SEPARATOR = "=";

    public ConfluentCloudCrnAuthority(int initialCacheCapacity) {
        super(AUTHORITY_NAME, initialCacheCapacity);
    }

    @Override
    protected String resolvePathElement(Element element) throws CrnSyntaxException {
        switch (element.resourceType()) {
            case ORGANIZATION_TYPE:
            case ENVIRONMENT_TYPE:
                // Because path elements are strings, we include the type in the string
                // to facilitate roles that define path-based scope levels
                return element.resourceType() + PATH_TYPE_SEPARATOR + element.encodedResourceName();
            default:
                throw new CrnSyntaxException(element.toString(),
                        String.format("Path element must be %s or %s",
                                ORGANIZATION_TYPE, ENVIRONMENT_TYPE));
        }
    }

    @Override
    protected void parsePathElements(List<String> path, Builder builder) throws CrnSyntaxException {
        for (String pathElement : path) {
            String[] parts = pathElement.split(PATH_TYPE_SEPARATOR);
            if (parts.length != 2 || parts[1].isEmpty()) {
                throw new CrnSyntaxException(pathElement,
                        String.format("Expected type%sname", PATH_TYPE_SEPARATOR));
            }
            switch (parts[0]) {
                case ORGANIZATION_TYPE:
                    builder.addElement(ORGANIZATION_TYPE, parts[1]);
                    break;
                case ENVIRONMENT_TYPE:
                    builder.addElement(ENVIRONMENT_TYPE, parts[1]);
                    break;
                default:
                    throw new CrnSyntaxException(pathElement,
                            String.format("Path element must be %s or %s",
                                    ORGANIZATION_TYPE, ENVIRONMENT_TYPE));
            }
        }
    }

    @Override
    public ConfluentResourceName canonicalCrn(Scope scope, ResourcePattern resourcePattern) throws CrnSyntaxException {
        ConfluentResourceName crn = super.canonicalCrn(scope, resourcePattern);

        // All Cloud resources belong to an organization, even if it's Org 0 = Confluent
        if (scope.path().isEmpty() || !scope.path().get(0).startsWith(ORGANIZATION_TYPE + PATH_TYPE_SEPARATOR)) {
            // Call appropriate CC service to resolve this, but for now...
            throw new CrnSyntaxException(scope.path().toString(), "Missing Organization");
        }
        // All Cloud clusters belong to an environment
        if (!scope.clusters().isEmpty() && (scope.path().size() < 2 ||
                !scope.path().get(1).startsWith(ENVIRONMENT_TYPE + PATH_TYPE_SEPARATOR))) {
            // Call appropriate CC service to resolve this, but for now...
            throw new CrnSyntaxException(scope.path().toString(), "Missing Environment");
        }
        if (scope.path().size() > 2) {
            throw new CrnSyntaxException(scope.path().toString(), "Extraneous path element");
        }

        return crn;
    }
}
