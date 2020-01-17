#include "marrow/api.h"
#include <arrow/python/pyarrow.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

void load_pyarrow() {
    static bool loaded_pyarrow = false;
    if (!loaded_pyarrow) {
        if (arrow::py::import_pyarrow() < 0) {
            throw std::runtime_error("Failed to load pyarrow");
        }
        loaded_pyarrow = true;
    }
}

namespace pybind11 { namespace detail {
    template <> struct type_caster<std::shared_ptr<arrow::RecordBatch>> {
    public:
        PYBIND11_TYPE_CASTER(std::shared_ptr<arrow::RecordBatch>, _("pyarrow.RecordBatch"));

        /**
         * Conversion part 1 (Python->C++): convert a PyObject into a inty
         * instance or return false upon failure. The second argument
         * indicates whether implicit conversions should be applied.
         */
        bool load(handle src, bool) {
            /* Extract PyObject from handle */
            PyObject *source = src.ptr();
            std::shared_ptr<arrow::RecordBatch> ret;
            auto status = arrow::py::unwrap_record_batch(source, &ret);
            if (!status.ok()) {
                return false;
            }
            value = ret;
            return true;
        }

        /**
         * Conversion part 2 (C++ -> Python): convert an inty instance into
         * a Python object. The second and third arguments are used to
         * indicate the return value policy and parent object (for
         * ``return_value_policy::reference_internal``) and are generally
         * ignored by implicit casters.
         */
        static handle cast(std::shared_ptr<arrow::RecordBatch> batch, return_value_policy /* policy */, handle /* parent */) {
            return arrow::py::wrap_record_batch(batch);
        }
    };
}} // namespace pybind11::detail

PYBIND11_MODULE(pymarrow, m) {
    load_pyarrow();
    m.def("add_index", &marrow::api::add_index, "Add an index column and meta data, which can be used by the sort and merge methods.", pybind11::arg("batch"), pybind11::arg("on"));
    m.def("sort", &marrow::api::sort, "Sort the record batch by the specified columns. If an index column is present it uses that.", pybind11::arg("batch"), pybind11::arg("on"));
    m.def("merge", &marrow::api::merge, "Do a left, inner or outer merge. If the table has either an index or is sorted (and has the required meta data as added by the add_index and sort methods), it will use those, otherwise it will create a temporary index",
        pybind11::arg("left"), pybind11::arg("right"), pybind11::arg("on"), pybind11::arg("how"), pybind11::arg("right_postfix") = "");

}
