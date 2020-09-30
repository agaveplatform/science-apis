<?php  if ( ! defined('BASEPATH')) exit('No direct script access allowed');

/****************************************************************************************
 ****************************************************************************************
 *
 *						Agave Postits Service Configuration File
 *
 ****************************************************************************************
 ****************************************************************************************/

date_default_timezone_set('America/Chicago');

$config['debug'] = envVar('DEBUG', false);
$config['service.version'] = envVar('IPLANT_SERVICE_VERSION','2.2.27-rf24a0c0');
$config['service.default.page.size'] = intval(envVar('IPLANT_DEFAULT_PAGE_SIZE', '100'));

/****************************************************************************************
 *						Trusted Users
 ****************************************************************************************/

$config['iplant.service.trusted.users'][] = 'dooley';

$config['iplant.service.trusted.domains'][] = 'agaveplatform.org';

/****************************************************************************************
 *						Logging keys
 ****************************************************************************************/
 
$config['iplant.service.log.servicekey'] = 'POSTITS02';
$config['iplant.service.log.activitykey']['create'] = 'PostItsAdd';
$config['iplant.service.log.activitykey']['redeem'] = 'PostItRedeem';
$config['iplant.service.log.activitykey']['revoke'] = 'PostItsDelete';
$config['iplant.service.log.activitykey']['list'] = 'PostItList';

/****************************************************************************************
 *						Database Connection Properties
 ****************************************************************************************/
 
$config['iplant.database.host'] = envVar('MYSQL_HOST', 'mysql');
$config['iplant.database.username'] = envVar('MYSQL_USERNAME','agaveuser');
$config['iplant.database.password'] = envVar('MYSQL_PASSWORD','password');
$config['iplant.database.name'] = envVar('MYSQL_DATABASE','agavecore');
$config['iplant.database.postits.table.name'] = envVar('IPLANT_DB_POSTITS_TABLE','postits');

/****************************************************************************************
 *						Agave API Service Endpoints
 ****************************************************************************************/
$config['iplant.foundation.services']['proxy'] = addTrailingSlash(envVar('IPLANT_PROXY_SERVICE', 'https://docker.example.com'));
$config['iplant.foundation.services']['auth'] = addTrailingSlash(envVar('IPLANT_AUTH_SERVICE', 'https://docker.example.com/auth/v2'));
$config['iplant.foundation.services']['io'] = addTrailingSlash(envVar('IPLANT_IO_SERVICE', 'https://docker.example.com/files/v2'));
$config['iplant.foundation.services']['apps'] = addTrailingSlash(envVar('IPLANT_APPS_SERVICE', 'https://docker.example.com/apps/v2'));
$config['iplant.foundation.services']['postit'] = addTrailingSlash(envVar('IPLANT_POSTIT_SERVICE', 'https://docker.example.com/postits/v2'));
$config['iplant.foundation.services']['profile'] = addTrailingSlash(envVar('IPLANT_PROFILE_SERVICE', 'https://docker.example.com/profiles/v2'));
$config['iplant.foundation.services']['log'] = addTrailingSlash(envVar('IPLANT_LOG_SERVICE', 'http://logging/logging/'));
 
/****************************************************************************************
 *						Error Response Codes
 ****************************************************************************************/
 
define( 'ERROR_200', 'HTTP/1.0 200 OK');
define( 'ERROR_400', 'HTTP/1.0 400 Bad Request');
define( 'ERROR_401', 'HTTP/1.0 401 Unauthorized');
define( 'ERROR_403', 'HTTP/1.0 403 Forbidden');
define( 'ERROR_405', 'HTTP/1.0 405 Method Not Allowed');
define( 'ERROR_404', 'HTTP/1.0 404 Not Found');
define( 'ERROR_500', 'HTTP/1.0 500 Internal Server Error');
define( 'ERROR_501', 'HTTP/1.0 501 Not Implemented');

//if (!function_exists('getEnv')) {
	function envVar($varName, $default='') {
		if (empty($varName)) {
			return $default;
		} else {
			$envVarName = strtoupper($varName);
			$envVarName = str_replace('.', '_', $varName);
			$val = getenv($envVarName);
			return (empty($val) ? $default : $val);
		}
	}
//}

function addTrailingSlash($value) {
	if (!endsWith($value, '/')) {
		$value .= '/';
	}
	return $value;
}
function endsWith($haystack,$needle,$case=true) {
    if($case){return (strcmp(substr($haystack, strlen($haystack) - strlen($needle)),$needle)===0);}
    return (strcasecmp(substr($haystack, strlen($haystack) - strlen($needle)),$needle)===0);
}
?>