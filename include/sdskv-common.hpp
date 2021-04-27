#ifndef __SDSKV_COMMON_HPP
#define __SDSKV_COMMON_HPP

#include <sdskv-common.h>

namespace sdskv {

/**
 * @brief Exception thrown when an sdskv call fails with a return code != 0.
 */
class exception : public std::exception {

    std::string m_msg;
    int         m_error;

  public:
    exception(int error) : m_error(error)
    {
        if (error > 0 && error < SDSKV_ERR_MAX) {
            m_msg = std::string("[SDSKV] ") + sdskv_error_messages[error];
        } else if (SDSKV_ERROR_IS_HG(error)) {
            m_msg = std::string("[SDSKV] Mercury error ")
                  + std::to_string(SDSKV_GET_HG_ERROR(error));
        } else if (SDSKV_ERROR_IS_ABT(error)) {
            m_msg = std::string("[SDSKV] Argobots error ")
                  + std::to_string(SDSKV_GET_ABT_ERROR(error));
        } else {
            m_msg = std::string("[SDSKV] Unknown error code ")
                  + std::to_string(error);
        }
    }

    virtual const char* what() const noexcept override { return m_msg.c_str(); }

    int error() const { return m_error; }
};

} // namespace sdskv

#endif
