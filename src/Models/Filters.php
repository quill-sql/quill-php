<?php
namespace Quill;
// Constants
define('IS_EXACTLY', 'is exactly');
define('IS_NOT_EXACTLY', 'is not exactly');
define('CONTAINS', 'contains');
define('IS', 'is');
define('IS_NOT', 'is not');
define('IS_NOT_NULL', 'is not null');
define('IS_NULL', 'is null');

define('IN_THE_LAST', 'in the last');
define('IN_THE_PREVIOUS', 'in the previous');
define('IN_THE_CURRENT', 'in the current');

define('EQUAL_TO', 'equal to');
define('NOT_EQUAL_TO', 'not equal to');
define('GREATER_THAN', 'greater than');
define('LESS_THAN', 'less than');
define('GREATER_THAN_OR_EQUAL_TO', 'greater than or equal to');
define('LESS_THAN_OR_EQUAL_TO', 'less than or equal to');

define('YEAR', 'year');
define('QUARTER', 'quarter');
define('MONTH', 'month');
define('WEEK', 'week');
define('DAY', 'day');
define('HOUR', 'hour');

define('NUMBER', 'number');
define('STRING', 'string');
define('DATE', 'date');
define('CUSTOM', 'custom');
define('BOOLEAN', 'boolean');

// Enums
abstract class StringOperator {
    const IS_EXACTLY = IS_EXACTLY;
    const IS_NOT_EXACTLY = IS_NOT_EXACTLY;
    const CONTAINS = CONTAINS;
    const IS = IS;
    const IS_NOT = IS_NOT;
}

abstract class DateOperator {
    const CUSTOM = CUSTOM;
    const IN_THE_LAST = IN_THE_LAST;
    const IN_THE_PREVIOUS = IN_THE_PREVIOUS;
    const IN_THE_CURRENT = IN_THE_CURRENT;
    const EQUAL_TO = EQUAL_TO;
    const NOT_EQUAL_TO = NOT_EQUAL_TO;
    const GREATER_THAN = GREATER_THAN;
    const LESS_THAN = LESS_THAN;
    const GREATER_THAN_OR_EQUAL_TO = GREATER_THAN_OR_EQUAL_TO;
    const LESS_THAN_OR_EQUAL_TO = LESS_THAN_OR_EQUAL_TO;
}

abstract class NumberOperator {
    const EQUAL_TO = EQUAL_TO;
    const NOT_EQUAL_TO = NOT_EQUAL_TO;
    const GREATER_THAN = GREATER_THAN;
    const LESS_THAN = LESS_THAN;
    const GREATER_THAN_OR_EQUAL_TO = GREATER_THAN_OR_EQUAL_TO;
    const LESS_THAN_OR_EQUAL_TO = LESS_THAN_OR_EQUAL_TO;
}

abstract class NullOperator {
    const IS_NOT_NULL = IS_NOT_NULL;
    const IS_NULL = IS_NULL;
}

abstract class BoolOperator {
    const EQUAL_TO = EQUAL_TO;
    const NOT_EQUAL_TO = NOT_EQUAL_TO;
}

abstract class TimeUnit {
    const YEAR = YEAR;
    const QUARTER = QUARTER;
    const MONTH = MONTH;
    const WEEK = WEEK;
    const DAY = DAY;
    const HOUR = HOUR;
}

abstract class FieldType {
    const STRING = STRING;
    const NUMBER = NUMBER;
    const DATE = DATE;
    const NULL = 'null';
    const BOOLEAN = BOOLEAN;
}

abstract class FilterType {
    const STRING_FILTER = 'string-filter';
    const DATE_FILTER = 'date-filter';
    const DATE_CUSTOM_FILTER = 'date-custom-filter';
    const DATE_COMPARISON_FILTER = 'date-comparison-filter';
    const NUMERIC_FILTER = 'numeric-filter';
    const NULL_FILTER = 'null-filter';
    const STRING_IN_FILTER = 'string-in-filter';
    const BOOLEAN_FILTER = 'boolean-filter';
}

// Base Filter Class
class DateRange {
    public $startDate;
    public $endDate;

    public function __construct($startDate, $endDate) {
        $this->startDate = $startDate;
        $this->endDate = $endDate;
    }
}

class DateValue {
    public $value;
    public $unit;

    public function __construct($value, $unit) {
        $this->value = $value;
        $this->unit = $unit;
    }
}

class BaseFilter {
    public $filterType;
    public $fieldType;
    public $operator;
    public $field;
    public $value;
    public $table;

    public function __construct($filterType, $fieldType, $operator, $field, $value, $table = null) {
        $this->filterType = $filterType;
        $this->fieldType = $fieldType;
        $this->operator = $operator;
        $this->field = $field;
        $this->value = $value;
        $this->table = $table;
    }

    public function toArray() {
        return get_object_vars($this);
    }
}

class Filter {
    public $filterType;
    public $operator;
    public $value;
    public $field;
    public $table;

    public function __construct($filterType, $operator, $value, $field, $table) {
        $this->filterType = $filterType;
        $this->operator = $operator;
        $this->value = $value;
        $this->field = $field;
        $this->table = $table;
    }

    public function toArray() {
        return get_object_vars($this);
    }
}

function convert_custom_filter(Filter $filter) {
    switch ($filter->filterType) {
        case FilterType::STRING_FILTER:
            if (!is_string($filter->value)) {
                throw new InvalidArgumentException('Invalid value for StringFilter, expected string');
            }
            if (!in_array($filter->operator, (new ReflectionClass(StringOperator::class))->getConstants())) {
                throw new InvalidArgumentException('Invalid operator for StringFilter, expected StringOperator');
            }
            return (new BaseFilter($filter->filterType, FieldType::STRING, $filter->operator, $filter->field, $filter->value, $filter->table))->toArray();

        case FilterType::STRING_IN_FILTER:
            if (!is_array($filter->value)) {
                throw new InvalidArgumentException('Invalid value for StringInFilter, expected array');
            }
            if (!in_array($filter->operator, (new ReflectionClass(StringOperator::class))->getConstants())) {
                throw new InvalidArgumentException('Invalid operator for StringInFilter, expected StringOperator');
            }
            return (new BaseFilter($filter->filterType, FieldType::STRING, $filter->operator, $filter->field, $filter->value, $filter->table))->toArray();

        case FilterType::NUMERIC_FILTER:
            if (!is_int($filter->value)) {
                throw new InvalidArgumentException('Invalid value for NumericFilter, expected int');
            }
            if (!in_array($filter->operator, (new ReflectionClass(NumberOperator::class))->getConstants())) {
                throw new InvalidArgumentException('Invalid operator for NumericFilter, expected NumberOperator');
            }
            return (new BaseFilter($filter->filterType, FieldType::NUMBER, $filter->operator, $filter->field, $filter->value, $filter->table))->toArray();

        case FilterType::DATE_FILTER:
            if (!($filter->value instanceof DateValue)) {
                throw new InvalidArgumentException('Invalid value for DateFilter, expected DateValue');
            }
            if (!in_array($filter->operator, (new ReflectionClass(DateOperator::class))->getConstants())) {
                throw new InvalidArgumentException('Invalid operator for DateFilter, expected DateOperator');
            }
            return (new BaseFilter($filter->filterType, FieldType::DATE, $filter->operator, $filter->field, $filter->value, $filter->table))->toArray();

        case FilterType::DATE_CUSTOM_FILTER:
            if (!($filter->value instanceof DateRange)) {
                throw new InvalidArgumentException('Invalid value for DateCustomFilter, expected DateRange');
            }
            if (!in_array($filter->operator, (new ReflectionClass(DateOperator::class))->getConstants())) {
                throw new InvalidArgumentException('Invalid operator for DateCustomFilter, expected DateOperator');
            }
            return (new BaseFilter($filter->filterType, FieldType::DATE, $filter->operator, $filter->field, $filter->value, $filter->table))->toArray();

        case FilterType::DATE_COMPARISON_FILTER:
            if (!is_string($filter->value)) {
                throw new InvalidArgumentException('Invalid value for DateComparisonFilter, expected string');
            }
            if (!in_array($filter->operator, (new ReflectionClass(DateOperator::class))->getConstants())) {
                throw new InvalidArgumentException('Invalid operator for DateComparisonFilter, expected DateOperator');
            }
            return (new BaseFilter($filter->filterType, FieldType::DATE, $filter->operator, $filter->field, $filter->value, $filter->table))->toArray();

        case FilterType::NULL_FILTER:
            if ($filter->value !== null) {
                throw new InvalidArgumentException('Invalid value for NullFilter, expected null');
            }
            if (!in_array($filter->operator, (new ReflectionClass(NullOperator::class))->getConstants())) {
                throw new InvalidArgumentException('Invalid operator for NullFilter, expected NullOperator');
            }
            return (new BaseFilter($filter->filterType, FieldType::NULL, $filter->operator, $filter->field, $filter->value, $filter->table))->toArray();

        case FilterType::BOOLEAN_FILTER:
            if (!is_bool($filter->value)) {
                throw new InvalidArgumentException('Invalid value for BooleanFilter, expected bool');
            }
            if (!in_array($filter->operator, (new ReflectionClass(BoolOperator::class))->getConstants())) {
                throw new InvalidArgumentException('Invalid operator for BooleanFilter, expected BoolOperator');
            }
            return (new BaseFilter($filter->filterType, FieldType::BOOLEAN, $filter->operator, $filter->field, $filter->value, $filter->table))->toArray();
        
        default:
            throw new InvalidArgumentException('Unknown filter type: ' . $filter->filterType);
    }
}
?>

