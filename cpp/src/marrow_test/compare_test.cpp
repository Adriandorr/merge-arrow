//
// Created by adorr on 11/01/2020.
//

#include "gtest/gtest.h"
#include "batch_maker.h"
#include "test_helpers.h"
#include "marrow/compare.h"
#include <arrow/type_traits.h>


class CompareTest : public testing::Test {
public:
};

TEST(CompareTest, TestSimple) {
    auto array =  BatchMaker().add_array<arrow::Int32Type>("a", {1, 2, 1, 4, 5, 3}).array<arrow::Int32Array>();
    marrow::SimpleComparer<arrow::Int32Array> comparer(array, array);
    ASSERT_TRUE(comparer.lt(0, 1));
    ASSERT_FALSE(comparer.lt(0, 2));
    ASSERT_FALSE(comparer.lt(1, 2));
    ASSERT_FALSE(comparer.lt(1, 0));
    ASSERT_TRUE(comparer.lt(2, 1));

    ASSERT_FALSE(comparer.gt(0, 1));
    ASSERT_FALSE(comparer.gt(0, 2));
    ASSERT_TRUE(comparer.gt(1, 2));
    ASSERT_TRUE(comparer.gt(1, 0));
    ASSERT_FALSE(comparer.gt(2, 1));
}

TEST(CompareTest, TestWithTwoArrays) {
    auto array1 =  BatchMaker().add_array<arrow::Int32Type>("a", {1, 2, 3}).array<arrow::Int32Array>();
    auto array2 =  BatchMaker().add_array<arrow::Int32Type>("a", {3, 2, 1}).array<arrow::Int32Array>();
    marrow::SimpleComparer<arrow::Int32Array> comparer(array1, array2);
    ASSERT_TRUE(comparer.lt(0, 0));
    ASSERT_FALSE(comparer.gt(0, 2));
    ASSERT_TRUE(comparer.lt(0, 1));
    ASSERT_FALSE(comparer.lt(2, 1));
    ASSERT_TRUE(comparer.gt(2, 1));
    ASSERT_FALSE(comparer.lt(1, 2));
    ASSERT_TRUE(comparer.gt(1, 2));
}

template<typename TArrowType>
class CompareScalarTest : public testing::Test {

};

TYPED_TEST_CASE(CompareScalarTest, ScalarTypes);

TYPED_TEST(CompareScalarTest, TestWithoutNulls) {
    typedef arrow::TypeTraits<TypeParam> TypeTrait;
    auto array = BatchMaker().add_array<TypeParam>("", {1, 2, 1, 4}).array();
    SCOPED_TRACE(array->ToString());
    marrow::SimpleComparer<typename TypeTrait::ArrayType> comparer(array, array);
    ASSERT_TRUE(comparer.lt(0, 1));
    ASSERT_FALSE(comparer.lt(0, 2));
    ASSERT_FALSE(comparer.lt(1, 2));
    ASSERT_FALSE(comparer.lt(1, 0));
    ASSERT_FALSE(comparer.lt(0, 2));
    ASSERT_TRUE(comparer.lt(2, 1));
    ASSERT_FALSE(comparer.gt(0, 1));
    ASSERT_FALSE(comparer.gt(0, 2));
    ASSERT_TRUE(comparer.gt(1, 2));
    ASSERT_TRUE(comparer.gt(1, 0));
    ASSERT_FALSE(comparer.gt(2, 1));
}

TYPED_TEST(CompareScalarTest, TestWitNulls) {
    typedef arrow::TypeTraits<TypeParam> TypeTrait;
    auto array = BatchMaker().add_array<TypeParam>("", {1, 2, 0, 4}).array();
    SCOPED_TRACE(array->ToString());
    marrow::SimpleComparer<typename TypeTrait::ArrayType> comparer(array, array);
    ASSERT_TRUE(comparer.lt(0, 1));
    ASSERT_FALSE(comparer.lt(0, 2));
    ASSERT_FALSE(comparer.lt(1, 2));
    ASSERT_FALSE(comparer.lt(1, 0));
    ASSERT_TRUE(comparer.lt(2, 0));
    ASSERT_TRUE(comparer.lt(2, 1));
    ASSERT_FALSE(comparer.gt(0, 1));
    ASSERT_TRUE(comparer.gt(0, 2));
    ASSERT_TRUE(comparer.gt(1, 2));
    ASSERT_TRUE(comparer.gt(1, 0));
    ASSERT_FALSE(comparer.gt(2, 0));
    ASSERT_FALSE(comparer.gt(2, 1));
}

TYPED_TEST(CompareScalarTest, TestMakeComparerWithNulls) {
    typedef arrow::TypeTraits<TypeParam> TypeTrait;
    auto array = BatchMaker().add_array<TypeParam>("", {1, 2, 0, 4}).array();
    SCOPED_TRACE(array->ToString());
    auto comparer = marrow::make_array_comparer(array, array);
    {
        SCOPED_TRACE(typeid(*comparer).name());
        ASSERT_EQ(typeid(*comparer), typeid(marrow::NullComparer));
    }
    ASSERT_TRUE(comparer->lt(0, 1));
    ASSERT_FALSE(comparer->lt(0, 2));
    ASSERT_FALSE(comparer->lt(1, 2));
    ASSERT_FALSE(comparer->lt(1, 0));
    ASSERT_TRUE(comparer->lt(2, 0));
    ASSERT_TRUE(comparer->lt(2, 1));
}

TYPED_TEST(CompareScalarTest, TestMakeComparer) {
    typedef arrow::TypeTraits<TypeParam> TypeTrait;
    auto array = BatchMaker().add_array<TypeParam>("", {1, 2, 1, 4}).array();
    SCOPED_TRACE(array->ToString());
    auto comparer = marrow::make_array_comparer(array, array);
    {
        SCOPED_TRACE(typeid(*comparer).name());
        ASSERT_EQ(typeid(*comparer), typeid(marrow::SimpleComparer<typename TypeTrait::ArrayType>));
    }
}

template<typename TArrowType>
class CompareStringDictTest : public testing::Test {

};

using DictIndexTypes = ::testing::Types<arrow::Int8Type, arrow::Int16Type, arrow::Int32Type>;


TYPED_TEST_CASE(CompareStringDictTest, DictIndexTypes);

TYPED_TEST(CompareStringDictTest, TestWithoutNull) {
    typedef arrow::TypeTraits<TypeParam> TypeTrait;
    auto array = BatchMaker().add_dict_array<TypeParam>("", {"1", "2", "1", "4"}).array();
    SCOPED_TRACE(array->ToString());
    auto comparer = marrow::make_array_comparer(array, array);
    {
        SCOPED_TRACE(typeid(*comparer).name());
        ASSERT_EQ(typeid(*comparer), typeid(marrow::StringComparer));
    }
    ASSERT_TRUE(comparer->lt(0, 1));
    ASSERT_FALSE(comparer->lt(0, 2));
    ASSERT_FALSE(comparer->lt(1, 2));
    ASSERT_FALSE(comparer->lt(1, 0));
    ASSERT_TRUE(comparer->lt(2, 1));
    ASSERT_FALSE(comparer->lt(2, 0));

    ASSERT_FALSE(comparer->gt(0, 1));
    ASSERT_FALSE(comparer->gt(0, 2));
    ASSERT_TRUE(comparer->gt(1, 2));
    ASSERT_TRUE(comparer->gt(1, 0));
    ASSERT_FALSE(comparer->gt(2, 1));
    ASSERT_FALSE(comparer->gt(2, 0));

}

TYPED_TEST(CompareStringDictTest, TestWithNull) {
    typedef arrow::TypeTraits<TypeParam> TypeTrait;
    auto array = BatchMaker().add_dict_array<TypeParam>("", {"1", "2", "", "4"}).array();
    SCOPED_TRACE(array->ToString());
    auto comparer = marrow::make_array_comparer(array, array);
    ASSERT_TRUE(comparer->lt(0, 1));
    ASSERT_FALSE(comparer->lt(0, 2));
    ASSERT_FALSE(comparer->lt(1, 2));
    ASSERT_FALSE(comparer->lt(1, 0));
    ASSERT_FALSE(comparer->lt(0, 2));
    ASSERT_TRUE(comparer->lt(2, 0));

    ASSERT_TRUE(comparer->gt(0, 2));
    ASSERT_TRUE(comparer->gt(1, 2));
    ASSERT_TRUE(comparer->gt(1, 0));
    ASSERT_FALSE(comparer->gt(2, 0));
}

template<typename TArrowType>
class CompareStringTest : public testing::Test {

};

using StringTypes = ::testing::Types<arrow::StringType, arrow::LargeStringType>;


TYPED_TEST_CASE(CompareStringTest, StringTypes);

TYPED_TEST(CompareStringTest, TestWithoutNull) {
    typedef arrow::TypeTraits<TypeParam> TypeTrait;
    auto array = BatchMaker().add_string_array<TypeParam>("", {"1", "2", "1", "4"}).array();
    SCOPED_TRACE(array->ToString());
    auto comparer = marrow::make_array_comparer(array, array);
    ASSERT_EQ(typeid(*comparer), typeid(marrow::StringComparer));
    ASSERT_TRUE(comparer->lt(0, 1));
    ASSERT_FALSE(comparer->lt(0, 2));
    ASSERT_FALSE(comparer->lt(1, 2));
    ASSERT_FALSE(comparer->lt(1, 0));
    ASSERT_FALSE(comparer->lt(0, 2));
    ASSERT_TRUE(comparer->lt(2, 1));
    ASSERT_FALSE(comparer->lt(2, 0));

    ASSERT_TRUE(!comparer->gt(0, 1));
    ASSERT_FALSE(comparer->gt(0, 2));
    ASSERT_FALSE(!comparer->gt(1, 2));
    ASSERT_FALSE(!comparer->gt(1, 0));
    ASSERT_FALSE(comparer->gt(0, 2));
    ASSERT_TRUE(!comparer->gt(2, 1));
    ASSERT_FALSE(comparer->gt(2, 0));
}

TYPED_TEST(CompareStringTest, TestWithNull) {
    typedef arrow::TypeTraits<TypeParam> TypeTrait;
    auto array = BatchMaker().add_string_array<TypeParam>("", {"1", "2", "", "4"}).array();
    SCOPED_TRACE(array->ToString());
    auto comparer = marrow::make_array_comparer(array, array);
    ASSERT_TRUE(comparer->lt(0, 1));
    ASSERT_FALSE(comparer->lt(0, 2));
    ASSERT_FALSE(comparer->lt(1, 2));
    ASSERT_FALSE(comparer->lt(1, 0));
    ASSERT_FALSE(comparer->lt(0, 2));
    ASSERT_TRUE(comparer->lt(2, 0));

    ASSERT_FALSE(!comparer->gt(0, 2));
    ASSERT_FALSE(!comparer->gt(1, 2));
    ASSERT_FALSE(!comparer->gt(1, 0));
    ASSERT_FALSE(!comparer->gt(0, 2));
    ASSERT_TRUE(!comparer->gt(2, 0));
}

class TextColumnComparer : public testing::Test {

};

TEST_F(TextColumnComparer, TestMultiColumns) {
    auto batch = BatchMaker()
            .add_array<>("b", {100, 150, 99, 200, 100})
            .add_array<>("c", {5, 4, 3, 2, 1})
            .add_string_array<>("a", {"1", "2", "2", "2", "4"})
            .record_batch();
    auto comparer = marrow::make_comparer(batch, {"a", "b"});
    ASSERT_TRUE(comparer->lt(0, 1));
    ASSERT_TRUE(comparer->lt(0, 2));
    ASSERT_FALSE(comparer->lt(1, 2));
    ASSERT_TRUE(comparer->lt(2, 1));
    ASSERT_TRUE(comparer->lt(1, 3));
    ASSERT_FALSE(comparer->lt(3, 1));
    ASSERT_TRUE(comparer->lt(2, 3));
    ASSERT_TRUE(comparer->lt(2, 4));
    ASSERT_FALSE(comparer->lt(4, 2));

    ASSERT_TRUE(!comparer->gt(0, 1));
    ASSERT_TRUE(!comparer->gt(0, 2));
    ASSERT_FALSE(!comparer->gt(1, 2));
    ASSERT_TRUE(!comparer->gt(2, 1));
    ASSERT_TRUE(!comparer->gt(1, 3));
    ASSERT_FALSE(!comparer->gt(3, 1));
    ASSERT_TRUE(!comparer->gt(2, 3));
    ASSERT_TRUE(!comparer->gt(2, 4));
    ASSERT_FALSE(!comparer->gt(4, 2));
}

TEST_F(TextColumnComparer, TestSingleColumns) {
    auto batch = BatchMaker()
            .add_array<>("b", {100, 150, 99, 200, 100})
            .add_array<>("c", {5, 4, 3, 2, 1})
            .add_string_array<>("a", {"1", "2", "2", "2", "4"})
            .record_batch();
    auto comparer = marrow::make_comparer(batch, {"a"});
    ASSERT_EQ(typeid(*comparer), typeid(marrow::StringComparer));
    ASSERT_TRUE(comparer->lt(0, 1));
}
