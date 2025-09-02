<?php

namespace Quill\Helpers;

class TenantHelper
{
    public static function extractTenantIds(array $tenants): array
    {
        if (empty($tenants)) {
            throw new \Exception("Invalid format for tenants");
        }

        if (is_string($tenants[0]) || is_numeric($tenants[0])) {
            return $tenants;
        }

        if (is_array($tenants[0]) && isset($tenants[0]['tenantIds'])) {
            // TODO: support multiple tenants in future
            return $tenants[0]['tenantIds'];
        }

        throw new \Exception("Invalid format for tenants");
    }
}