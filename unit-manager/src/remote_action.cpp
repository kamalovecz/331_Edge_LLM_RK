#include <string>
#include <simdjson.h>
#include "remote_action.h"
#include "zmq_bus.h"
#include "all.h"
#include <StackFlowUtil.h>
#include "json.hpp"

using namespace StackFlows;

/**
 * remote_call：处理非 inference 的 RPC 调用
 *
 * JSON 输入格式应包含：
 *  - "work_id"
 *  - "action"
 *
 * 本函数将调用 pzmq 执行 RPC 动作。
 */

int remote_call(int com_id, const std::string &json_str)
{
    ALOGI("remote_call input json: %s", json_str.c_str());

    simdjson::ondemand::parser parser;
    simdjson::padded_string json_string(json_str);
    simdjson::ondemand::document doc;

    auto error = parser.iterate(json_string).get(doc);
    if (error) {
        ALOGE("remote_call JSON parse error!");
        return -1;
    }

    std::string work_id;
    {
        auto res = doc["work_id"].get_string();
        if (res.error()) {
            ALOGE("remote_call missing work_id!");
            return -1;
        }
        work_id = std::string(res.value());
    }

    std::string action;
    {
        auto res = doc["action"].get_string();
        if (res.error()) {
            ALOGE("remote_call missing action!");
            return -1;
        }
        action = std::string(res.value());
    }

    if (work_id.empty() || action.empty()) {
        ALOGE("remote_call fields empty!");
        return -1;
    }

    // ---- 构造 RPC 地址 ----
    char com_url[128];
    sprintf(com_url, zmq_c_format.c_str(), com_id);

    // ---- 通过 pzmq 调用 RPC ----
    pzmq client(work_id);

    ALOGI("remote_call: work_id=%s, action=%s, com_url=%s",
          work_id.c_str(), action.c_str(), com_url);

    return client.call_rpc_action(
        action,
        pzmq_data::set_param(com_url, json_str),
        nullptr
    );
}
