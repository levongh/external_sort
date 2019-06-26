#pragma once

namespace external_sort {

template <typename InputStream, typename OutputStream>
void copy_stream(InputStream* sin, OutputStream* sout)
{
    while (!sin->empty()) {
        sout->Push(sin->Front());
        sin->Pop();
    }
}

template <typename InputStream, typename OutputStream, typename Comparator>
void merge_2streams(std::unordered_set<InputStream*>& sin, OutputStream* sout,
                    Comparator comp)
{
    if (sin.size() != 2) {
        return;
    }
    auto it = sin.begin();
    InputStream* s1 = *(it++);
    InputStream* s2 = *(it++);
    InputStream* smin = s1;

    for (;;) {
        smin = comp(s1->Front(), s2->Front()) ? s1 : s2;
        sout->Push(smin->Front());
        smin->Pop();
        if (smin->empty()) {
            sin.erase(smin);
            break;
        }
    }
    copy_stream(*sin.begin(), sout);
}

template <typename InputStream, typename OutputStream, typename Comparator>
void merge_3streams(std::unordered_set<InputStream*>& sin, OutputStream* sout,
                    Comparator comp)
{
    if (sin.size() != 3) {
        return;
    }
    auto it = sin.begin();
    InputStream* s1 = *(it++);
    InputStream* s2 = *(it++);
    InputStream* s3 = *(it++);
    InputStream* smin = s1;

    for (;;) {
        if (comp(s1->Front(),s2->Front())) {
            smin = comp(s1->Front(), s3->Front()) ? s1 : s3;
        } else {
            smin = comp(s2->Front(), s3->Front()) ? s2 : s3;
        }
        sout->Push(smin->Front());
        smin->Pop();
        if (smin->empty()) {
            sin.erase(smin);
            break;
        }
    }
    merge_2streams(sin, sout, comp);
}

// merges 4 streams
template <typename InputStream, typename OutputStream, typename Comparator>
void merge_4streams(std::unordered_set<InputStream*>& sin, OutputStream* sout,
                    Comparator comp)
{
    if (sin.size() != 4) {
        return;
    }
    auto it = sin.begin();
    InputStream* s1 = *(it++);
    InputStream* s2 = *(it++);
    InputStream* s3 = *(it++);
    InputStream* s4 = *(it++);
    InputStream* smin = s1;

    for (;;) {
        if (comp(s1->Front(), s2->Front())) {
            if (comp(s3->Front(), s4->Front()))
                smin = comp(s1->Front(), s3->Front()) ? s1 : s3;
            else
                smin = comp(s1->Front(), s4->Front()) ? s1 : s4;
        } else {
            if (comp(s3->Front(), s4->Front()))
                smin = comp(s2->Front(), s3->Front()) ? s2 : s3;
            else
                smin = comp(s2->Front(), s4->Front()) ? s2 : s4;
        }
        sout->Push(smin->Front());
        smin->Pop();
        if (smin->empty()) {
            sin.erase(smin);
            break;
        }
    }
    merge_3streams(sin, sout, comp);
}

template <typename InputStream, typename OutputStream, typename Comparator>
void merge_nstreams(std::unordered_set<InputStream*>& sin, OutputStream* sout,
                    Comparator comp)
{
    if (sin.size() <= 4) {
        return;
    }

    InputStream* smin;

    std::vector<InputStream*> heap;
    for (auto& s : sin) {
        if (!s->empty()) {
            heap.push_back(s);
        }
    }
    auto hcomp = [ &comp ] (InputStream*& s1, InputStream*& s2) {
        return comp(s2->Front(), s1->Front());
    };
    std::make_heap(heap.begin(), heap.end(), hcomp);

    while (heap.size() > 4) {
        // find minimum element in the input streams
        smin = heap.front();
        std::pop_heap(heap.begin(), heap.end(), hcomp);

        // output the minumum element
        sout->Push(smin->Front());
        smin->Pop();

        if (smin->empty()) {
            // end of this stream
            heap.pop_back();
            sin.erase(smin);
        } else {
            // there is more data in the stream,
            // push it back to the heap
            heap.back() = smin;
            std::push_heap(heap.begin(), heap.end(), hcomp);
        }
    }
    merge_4streams(sin, sout, comp);
}

template <typename InputStreamPtr, typename OutputStreamPtr>
OutputStreamPtr merge_streams(std::unordered_set<InputStreamPtr> sin,
                              OutputStreamPtr sout)
{
    using InputStream = typename InputStreamPtr::element_type;
    using OutputStream = typename OutputStreamPtr::element_type;
    std::unordered_set<InputStream*> sinp;
    OutputStream* soutp = sout.get();

    auto comp = std::less<typename InputStream::BlockType::value_type>();

    for (const auto& s : sin) {
        s->open();
        if (!s->empty()) {
            sinp.insert(s.get());
        }
    }

    if (sinp.size() > 0) {
        sout->Open();
        if (sinp.size() > 4) {
            merge_nstreams(sinp, soutp, comp);
        } else if (sinp.size() == 4) {
            merge_4streams(sinp, soutp, comp);
        } else if (sinp.size() == 3) {
            merge_3streams(sinp, soutp, comp);
        } else if (sinp.size() == 2) {
            merge_2streams(sinp, soutp, comp);
        } else if (sinp.size() == 1) {
            copy_stream(*sinp.begin(), soutp);
        }
        sout->Close();
    } else {
        //LOG_ERR(("No input streams to merge!"));
        sout.reset();
    }

    for (const auto& s : sin) {
        s->close();
    }
    return sout;
}

} // namespace external_sort

