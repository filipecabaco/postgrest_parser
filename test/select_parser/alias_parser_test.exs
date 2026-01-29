defmodule PostgrestParser.SelectParser.AliasParserTest do
  use ExUnit.Case, async: true

  alias PostgrestParser.SelectParser.AliasParser

  describe "parse_select_item/1 - simple aliases" do
    test "parses simple alias" do
      assert {alias_name, field_name} = AliasParser.parse("user_name:name")
      assert alias_name == "user_name"
      assert field_name == "name"
    end

    test "parses field without alias" do
      assert {nil, field_name} = AliasParser.parse("name")
      assert field_name == "name"
    end

    test "parses field with underscore" do
      assert {nil, field_name} = AliasParser.parse("user_name")
      assert field_name == "user_name"
    end

    test "parses alias with underscore" do
      assert {alias_name, field_name} = AliasParser.parse("my_alias:field_name")
      assert alias_name == "my_alias"
      assert field_name == "field_name"
    end
  end

  describe "parse_select_item/1 - type casts" do
    test "parses field with cast" do
      assert {nil, field_name} = AliasParser.parse("price::text")
      assert field_name == "price::text"
    end

    test "parses field with cast and alias" do
      assert {alias_name, field_name} = AliasParser.parse("price::text:price_str")
      assert alias_name == "price_str"
      assert field_name == "price::text"
    end

    test "parses field with numeric cast" do
      assert {nil, field_name} = AliasParser.parse("amount::numeric")
      assert field_name == "amount::numeric"
    end

    test "parses field with cast and alias with numeric cast" do
      assert {alias_name, field_name} = AliasParser.parse("amount::numeric:total")
      assert alias_name == "total"
      assert field_name == "amount::numeric"
    end

    test "parses field with timestamp cast" do
      assert {nil, field_name} = AliasParser.parse("created_at::timestamp")
      assert field_name == "created_at::timestamp"
    end
  end

  describe "parse_select_item/1 - JSON paths" do
    test "parses JSON path with arrow operator" do
      assert {nil, field_name} = AliasParser.parse("data->value")
      assert field_name == "data->value"
    end

    test "parses JSON path with double arrow operator" do
      assert {nil, field_name} = AliasParser.parse("data->>value")
      assert field_name == "data->>value"
    end

    test "parses JSON path with alias" do
      assert {alias_name, field_name} = AliasParser.parse("my_value:data->value")
      assert alias_name == "my_value"
      assert field_name == "data->value"
    end

    test "parses JSON path with double arrow and alias" do
      assert {alias_name, field_name} = AliasParser.parse("my_text:data->>text")
      assert alias_name == "my_text"
      assert field_name == "data->>text"
    end

    test "parses nested JSON path" do
      assert {nil, field_name} = AliasParser.parse("data->user->name")
      assert field_name == "data->user->name"
    end

    test "parses nested JSON path with alias" do
      assert {alias_name, field_name} = AliasParser.parse("username:data->user->name")
      assert alias_name == "username"
      assert field_name == "data->user->name"
    end
  end

  describe "parse_select_item/1 - JSON paths with casts" do
    test "parses JSON path with cast" do
      assert {nil, field_name} = AliasParser.parse("data->price::numeric")
      assert field_name == "data->price::numeric"
    end

    test "parses JSON path with cast and alias" do
      assert {alias_name, field_name} = AliasParser.parse("data->price::numeric:total")
      assert alias_name == "total"
      assert field_name == "data->price::numeric"
    end

    test "parses JSON path with double arrow, cast, and alias" do
      assert {alias_name, field_name} = AliasParser.parse("data->>amount::text:amount_str")
      assert alias_name == "amount_str"
      assert field_name == "data->>amount::text"
    end

    test "parses nested JSON path with cast" do
      assert {nil, field_name} = AliasParser.parse("data->user->age::integer")
      assert field_name == "data->user->age::integer"
    end

    test "parses nested JSON path with cast and alias" do
      assert {alias_name, field_name} =
               AliasParser.parse("data->user->age::integer:user_age")

      assert alias_name == "user_age"
      assert field_name == "data->user->age::integer"
    end
  end

  describe "parse_select_item/1 - edge cases" do
    test "handles field with multiple underscores" do
      assert {nil, field_name} = AliasParser.parse("user_full_name")
      assert field_name == "user_full_name"
    end

    test "handles alias with numbers" do
      assert {alias_name, field_name} = AliasParser.parse("alias123:field456")
      assert alias_name == "alias123"
      assert field_name == "field456"
    end

    test "handles JSON path with numbers in keys" do
      assert {nil, field_name} = AliasParser.parse("data->item1->value2")
      assert field_name == "data->item1->value2"
    end

    test "handles complex field with all features" do
      assert {alias_name, field_name} =
               AliasParser.parse("data->nested->value::text:my_alias123")

      assert alias_name == "my_alias123"
      assert field_name == "data->nested->value::text"
    end
  end

  describe "parse_select_item/1 - with hints and extra text" do
    test "parses field with hint (hint stays in field, will be extracted later)" do
      assert {nil, field_name} = AliasParser.parse("author!inner")
      assert field_name == "author!inner"
    end

    test "parses aliased field with hint (hint stays in field)" do
      assert {alias_name, field_name} = AliasParser.parse("display_name:author!inner")
      assert alias_name == "display_name"
      assert field_name == "author!inner"
    end

    test "parses field with cast and hint" do
      assert {nil, field_name} = AliasParser.parse("price::numeric!inner")
      assert field_name == "price::numeric!inner"
    end

    test "parses spread operator" do
      assert {nil, field_name} = AliasParser.parse("...profile")
      assert field_name == "...profile"
    end
  end

  describe "parse_select_item/1 - edge cases with empty" do
    test "handles empty string" do
      assert {nil, ""} = AliasParser.parse("")
    end

    test "handles wildcard" do
      assert {nil, "*"} = AliasParser.parse("*")
    end

    test "handles aliased wildcard" do
      assert {alias_name, field_name} = AliasParser.parse("all:*")
      assert alias_name == "all"
      assert field_name == "*"
    end
  end
end
