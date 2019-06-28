#pragma once

namespace external_sort {

template <typename IStream, typename OStream>
void copy_stream(IStream* sin, OStream* sout)
{
    while (!sin->empty()) {
        sout->push(sin->front());
        sin->pop();
    }
}

template <typename IStream, typename OStream, typename Comparator>
void merge_2streams(std::unordered_set<IStream*>& sin, OStream* sout,
                    Comparator comp)
{
    if (sin.size() != 2) {
        return;
    }
    auto it = sin.begin();
    IStream* s1 = *(it++);
    IStream* s2 = *(it++);
    IStream* smin = s1;

    for (;;) {
        smin = comp(s1->front(), s2->front()) ? s1 : s2;
        sout->push(smin->front());
        smin->pop();
        if (smin->empty()) {
            sin.erase(smin);
            break;
        }
    }
    copy_stream(*sin.begin(), sout);
}

template <typename IStream, typename OStream, typename Comparator>
void merge_3streams(std::unordered_set<IStream*>& sin, OStream* sout,
                    Comparator comp)
{
    if (sin.size() != 3) {
        return;
    }
    auto it = sin.begin();
    IStream* s1 = *(it++);
    IStream* s2 = *(it++);
    IStream* s3 = *(it++);
    IStream* smin = s1;

    for (;;) {
        if (comp(s1->front(),s2->front())) {
            smin = comp(s1->front(), s3->front()) ? s1 : s3;
        } else {
            smin = comp(s2->front(), s3->front()) ? s2 : s3;
        }
        sout->push(smin->front());
        smin->pop();
        if (smin->empty()) {
            sin.erase(smin);
            break;
        }
    }
    merge_2streams(sin, sout, comp);
}

// merges 4 streams
template <typename IStream, typename OStream, typename Comparator>
void merge_4streams(std::unordered_set<IStream*>& sin, OStream* sout,
                    Comparator comp)
{
    if (sin.size() != 4) {
        return;
    }
    auto it = sin.begin();
    IStream* s1 = *(it++);
    IStream* s2 = *(it++);
    IStream* s3 = *(it++);
    IStream* s4 = *(it++);
    IStream* smin = s1;

    for (;;) {
        if (comp(s1->front(), s2->front())) {
            if (comp(s3->front(), s4->front()))
                smin = comp(s1->front(), s3->front()) ? s1 : s3;
            else
                smin = comp(s1->front(), s4->front()) ? s1 : s4;
        } else {
            if (comp(s3->front(), s4->front()))
                smin = comp(s2->front(), s3->front()) ? s2 : s3;
            else
                smin = comp(s2->front(), s4->front()) ? s2 : s4;
        }
        sout->push(smin->front());
        smin->pop();
        if (smin->empty()) {
            sin.erase(smin);
            break;
        }
    }
    merge_3streams(sin, sout, comp);
}

template <typename IStream, typename OStream, typename Comparator>
void merge_nstreams(std::unordered_set<IStream*>& sin, OStream* sout,
                    Comparator comp)
{
    if (sin.size() <= 4) {
        return;
    }
    IStream* smin;
    std::vector<IStream*> heap;
    for (auto& s : sin) {
        if (!s->empty()) {
            heap.push_back(s);
        }
    }
    auto hcomp = [ &comp ] (IStream*& s1, IStream*& s2) {
        return comp(s2->front(), s1->front());
    };
    std::make_heap(heap.begin(), heap.end(), hcomp);

    while (heap.size() > 4) {
        smin = heap.front();
        std::pop_heap(heap.begin(), heap.end(), hcomp);

        sout->push(smin->front());
        smin->pop();

        if (smin->empty()) {
            heap.pop_back();
            sin.erase(smin);
        } else {
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
        sout->open();
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
        sout->close();
    } else {
        sout.reset();
    }

    for (const auto& s : sin) {
        s->close();
    }
    return sout;
}

} // namespace external_sort

