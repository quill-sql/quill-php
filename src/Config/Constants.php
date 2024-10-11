<?php

namespace Quill\Config;

class Constants
{
    public const ENV = 'production';
    public const HOST = self::ENV === 'development' ? 'http://localhost:8080' : 'https://quill-344421.uc.r.appspot.com';
}
